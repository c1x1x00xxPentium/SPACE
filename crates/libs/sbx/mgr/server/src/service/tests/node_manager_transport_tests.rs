//! Lifecycle handlers driven against an in-process node-manager.
//!
//! The service is built from [`SandboxManagerServiceOptions::for_test`], with
//! every host dependency redirected:
//!
//! - the node-manager connector reaches a [`FakeNodeManager`];
//! - every template lookup goes to a template-manager fake whose socket is
//!   unbound, so a lookup fails to connect (`TemplateLookupError::Connect`,
//!   which create reports as `Internal` "cannot connect to template-manager");
//! - the credential-manager socket is a test-owned path nothing binds;
//! - the host-control socket, which sandbox-manager only passes through to
//!   node-manager's `Launch`, names no live host service;
//! - no runtime is ever live.

use super::service_tests::{insert_test_sandbox, runtime_fields};
use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::activation_support::internal_request;
use pplx_space_common_core::types::ExecutionGeneration;
use pplx_space_node_mgr_client::proto::{
    CleanupSandboxRequest, CleanupSandboxResponse, StopProcessRequest, StopProcessResponse,
};

/// A test service whose node-manager clients all come from `node_manager`.
pub(crate) fn service_with_node_manager(
    conn: rusqlite::Connection,
    node_manager: NodeManagerConnector,
) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager,
            ..SandboxManagerServiceOptions::for_test()
        },
    )
}

#[tokio::test]
async fn fatal_kill_signal_failure_still_schedules_admitted_cleanup() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let node_manager = FakeNodeManager::default()
        .unary("StopProcess", {
            let stops = Arc::clone(&stops);
            move |request: StopProcessRequest| {
                stops.lock().unwrap().push(request);
                Err::<StopProcessResponse, _>(Status::unavailable("injected StopProcess refusal"))
            }
        })
        .unary("CleanupSandbox", {
            let cleanups = Arc::clone(&cleanups);
            move |request: CleanupSandboxRequest| {
                cleanups.lock().unwrap().push(request);
                Ok(CleanupSandboxResponse::default())
            }
        })
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_with_node_manager(conn, node_manager.connector());
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    let execution_fence = service
        .lookup_sandbox(&sandbox_id_string)
        .unwrap()
        .node_execution_fence();
    assert!(execution_fence.is_some());

    let error = service
        .kill(internal_request(KillRequest {
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            blocking: false,
        }))
        .await
        .expect_err("node-manager's refusal must fail fatal signal dispatch");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error.message().contains("send fatal signal failed")
            && error.message().contains("injected StopProcess refusal"),
        "the caller must see node-manager's refusal: {}",
        error.message()
    );
    // The signal reached node-manager once, fenced to the observed execution.
    assert_eq!(
        *stops.lock().unwrap(),
        [StopProcessRequest {
            execution_fence: execution_fence.clone(),
            sandbox_id: sandbox_id_string.clone(),
            signal: 9,
            pid: None,
        }]
    );

    // The cleanup admitted before the signal still runs: node-manager stops
    // the same execution, keeping its filesystem, and the row ends stopped.
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.lookup_sandbox(&sandbox_id_string).unwrap().state != SandboxState::Stopped {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fatal signal failure must still run the admitted cleanup to completion");
    assert_eq!(
        *cleanups.lock().unwrap(),
        [CleanupSandboxRequest {
            execution_fence,
            sandbox_id: sandbox_id_string,
            force: true,
            keep_filesystem: true,
            return_after_slot_release: true,
            immediate_kill: true,
        }]
    );
    assert_eq!(node_manager.calls(), ["StopProcess", "CleanupSandbox"]);
}

/// Template-manager falls back to an unclamped memory size when this read
/// fails, so a node-manager that is restarting must be waited out within the
/// connect budget the read always had, not failed after a short local retry.
#[tokio::test]
async fn memory_policy_read_waits_out_a_restarting_node_manager() {
    use pplx_space_node_mgr_client::proto::{NetworkPoolStatusRequest, NetworkPoolStatusResponse};
    let restarted_at = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let node_manager = FakeNodeManager::default()
        .unary(
            "GetNetworkPoolStatus",
            move |_: NetworkPoolStatusRequest| {
                if std::time::Instant::now() < restarted_at {
                    return Err(Status::unavailable("node-manager is restarting"));
                }
                Ok(NetworkPoolStatusResponse {
                    sandbox_cgroup_memory_mib: 4096,
                    sandbox_guest_memory_max_mib: 3584,
                    sandbox_memory_headroom_mib: 512,
                    ..Default::default()
                })
            },
        )
        .serve();
    let service = service_with_node_manager(crate::db::test_conn(), node_manager.connector());

    let resolved = service
        .resolve_sandbox_memory(internal_request(ResolveSandboxMemoryRequest {
            requested_memory_mib: 8192,
        }))
        .await
        .expect("the read outlasts the restart")
        .into_inner();
    assert_eq!(resolved.effective_memory_mib, 3584);
}

/// The connect budget never outlasts the caller: once its `grpc-timeout` has
/// passed, the read stops retrying a node-manager that is still down.
#[tokio::test]
async fn memory_policy_read_stops_retrying_at_the_caller_deadline() {
    use pplx_space_node_mgr_client::proto::{NetworkPoolStatusRequest, NetworkPoolStatusResponse};
    let node_manager = FakeNodeManager::default()
        .unary(
            "GetNetworkPoolStatus",
            |_: NetworkPoolStatusRequest| -> Result<NetworkPoolStatusResponse, Status> {
                Err(Status::unavailable("node-manager is restarting"))
            },
        )
        .serve();
    let service = service_with_node_manager(crate::db::test_conn(), node_manager.connector());
    let mut request = internal_request(ResolveSandboxMemoryRequest {
        requested_memory_mib: 8192,
    });
    request
        .metadata_mut()
        .insert("grpc-timeout", "500m".parse().unwrap());

    let started = std::time::Instant::now();
    service
        .resolve_sandbox_memory(request)
        .await
        .expect_err("node-manager never came back");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "retried for {:?} past a 500 ms caller deadline",
        started.elapsed()
    );
}
