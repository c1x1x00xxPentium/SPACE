//! Server-speaks-first flows under TLS interception. Origins listen on
//! 0.0.0.0 (policies refuse 127/8) and the dialer sets no socket mark, so no
//! test needs privilege or an external address.

use super::*;
use crate::tls_sni::test_fixtures::client_hello;
use futures_util::FutureExt as _;
use pplx_space_metrics::test_capture::{CapturedCounter, capture_counters_guard};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Past the 50 ms peek, so the guest writes while the probe is running.
const AFTER_PROBE_STARTS: Duration = Duration::from_millis(300);

type CertificateRequests = Arc<std::sync::Mutex<Vec<(DomainName, u16)>>>;

async fn dial(dst: SocketAddr) -> Result<TcpStream, transport::Error> {
    Ok(TcpStream::connect(dst).await?)
}

async fn must_not_dial(dst: SocketAddr) -> Result<TcpStream, transport::Error> {
    panic!("must not dial {dst}");
}

/// A proxy enforcing `policy`. Its certificate authority records and refuses
/// every request, so an intercepted flow ends at the handshake.
async fn proxy(policy: NetworkPolicyV1) -> (TcpProxy, CertificateRequests) {
    let (network_state, _) = ready_policy(policy).await;
    let requests = CertificateRequests::default();
    let recorded = Arc::clone(&requests);
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        tls_interception: TlsInterception::new(Arc::new(move |host, port| {
            recorded.lock().unwrap().push((host, port));
            Box::pin(async { Err(tonic::Status::unavailable("synthetic signer")) })
        })),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        known_egress_state: KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    (proxy, requests)
}

fn intercepted(policy: NetworkPolicyV1) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        tls_interception: pplx_space_net_common::TlsInterception::On,
        ..policy
    }
}

/// Interception on, with the destination admitted by its address alone.
fn ip_allow(destination: SocketAddr) -> NetworkPolicyV1 {
    intercepted(NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Ip(destination.ip())],
            Vec::new(),
        )
        .unwrap(),
    })
}

async fn guest_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (guest, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    (guest.unwrap(), accepted.unwrap().0)
}

async fn origin() -> (TcpListener, SocketAddr) {
    let origin = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let destination = origin.local_addr().unwrap();
    (origin, destination)
}

fn missing_sni_detail(outcome: &FlowOutcome) -> Option<&'static str> {
    match outcome {
        FlowOutcome::Rejected(Rejection::TlsInterception(error)) => interception_detail(error),
        _ => None,
    }
}

fn admission_decisions(counters: &[CapturedCounter]) -> u64 {
    counters
        .iter()
        .filter(|counter| {
            counter.name == "gateway.policy.decision"
                && counter.tags.iter().any(|tag| tag == "phase:admission")
        })
        .map(|counter| counter.value)
        .sum()
}

#[tokio::test]
async fn a_server_first_greeting_reaches_a_silent_guest_and_the_session_relays() {
    for (name, policy) in [
        ("IP allow", None),
        ("allow all", Some(intercepted(NetworkPolicyV1::allow_all()))),
    ] {
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(policy.unwrap_or_else(|| ip_allow(destination))).await;
        let (mut guest, gateway_guest) = guest_pair().await;

        let server = async {
            let (mut stream, _) = origin.accept().await.unwrap();
            stream
                .write_all(b"220 ftp.example.test ready\r\n")
                .await
                .unwrap();
            let mut command = [0_u8; 12];
            stream.read_exact(&mut command).await.unwrap();
            assert_eq!(&command, b"USER guest\r\n", "{name}");
            stream.write_all(b"331 password\r\n").await.unwrap();
        };
        let client = async {
            let mut greeting = [0_u8; 28];
            guest.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"220 ftp.example.test ready\r\n", "{name}");
            guest.write_all(b"USER guest\r\n").await.unwrap();
            guest.shutdown().await.unwrap();
            let mut reply = Vec::new();
            guest.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"331 password\r\n", "{name}");
        };
        let shutdown = CancellationToken::new();
        let counters = capture_counters_guard();
        let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                server,
                client,
            )
        })
        .await
        .expect(name);
        assert_eq!(admission_decisions(&counters.finish()), 1, "{name}");

        let FlowOutcome::Relayed(Relay::Tunnelled { wire_tx, wire_rx }) = outcome else {
            panic!("{name}: a server-first flow must relay");
        };
        assert_eq!(
            (wire_tx, wire_rx),
            (12, 28 + 14),
            "{name}: greeting is counted"
        );
        assert!(requests.lock().unwrap().is_empty(), "{name}");
        assert_eq!(proxy.stats.tcp_total.load(Ordering::Relaxed), 1, "{name}");
    }
}

/// A ClientHello sent after the probe starts is handled like an on-time one:
/// offered to interception, never relayed, and the probe connection gets
/// no bytes.
#[tokio::test]
async fn a_late_client_hello_is_inspected_and_the_probe_connection_sees_nothing() {
    for (name, server_name, policy) in [
        ("allowed SNI", "good.example.test", None),
        (
            "denied SNI",
            "blocked.example.test",
            Some(NetworkPolicyV1 {
                egress: EgressPolicyV1::new(
                    EgressDefaultAction::Allow,
                    Vec::new(),
                    vec![EgressDestinationV1::Domain(
                        "blocked.example.test".parse().unwrap(),
                    )],
                )
                .unwrap(),
                ..intercepted(NetworkPolicyV1::allow_all())
            }),
        ),
    ] {
        let hello = client_hello(Some(server_name));
        let mut outcomes = Vec::new();
        for late in [false, true] {
            let (origin, destination) = origin().await;
            let policy = policy.clone().unwrap_or_else(|| ip_allow(destination));
            let (proxy, requests) = proxy(policy).await;
            let (mut guest, gateway_guest) = guest_pair().await;
            let client = async {
                if late {
                    tokio::time::sleep(AFTER_PROBE_STARTS).await;
                }
                guest.write_all(&hello).await.unwrap();
            };
            let probe_connection = async {
                if !late {
                    return;
                }
                let (mut stream, _) = origin.accept().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(
                    received.is_empty(),
                    "{name}: the probe connection received guest bytes"
                );
            };
            let shutdown = CancellationToken::new();
            let counters = capture_counters_guard();
            let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(
                    proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                    client,
                    probe_connection,
                )
            })
            .await
            .expect(name);
            assert_eq!(
                admission_decisions(&counters.finish()),
                1,
                "{name}, late={late}: one admission decision per flow"
            );
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::TlsInterception(
                        TlsInterceptionError::CertificateRequest(_)
                    ))
                ),
                "{name}, late={late}: the ClientHello must reach interception"
            );
            assert_eq!(
                requests.lock().unwrap().as_slice(),
                [(server_name.parse().unwrap(), destination.port())],
                "{name}, late={late}"
            );
            assert!(
                origin.accept().now_or_never().is_none(),
                "{name}, late={late}: nothing may be relayed to the destination"
            );
            outcomes.push(outcome.metric_tags());
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "{name}: lateness changed the outcome"
        );
    }
}

#[tokio::test]
async fn late_plaintext_takes_the_existing_plaintext_path() {
    const REQUEST: &[u8] = b"PING example\r\n";
    let (origin, destination) = origin().await;
    let (proxy, requests) = proxy(ip_allow(destination)).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let client = async {
        tokio::time::sleep(AFTER_PROBE_STARTS).await;
        guest.write_all(REQUEST).await.unwrap();
        guest.shutdown().await.unwrap();
        let mut reply = Vec::new();
        guest.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"PONG\r\n");
    };
    let server = async {
        let (mut probe, _) = origin.accept().await.unwrap();
        let mut received = Vec::new();
        probe.read_to_end(&mut received).await.unwrap();
        assert!(
            received.is_empty(),
            "the probe connection received guest bytes"
        );
        let (mut relayed, _) = origin.accept().await.unwrap();
        let mut request = vec![0_u8; REQUEST.len()];
        relayed.read_exact(&mut request).await.unwrap();
        assert_eq!(request, REQUEST);
        relayed.write_all(b"PONG\r\n").await.unwrap();
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            client,
            server,
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        FlowOutcome::Relayed(Relay::Tunnelled { .. })
    ));
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_flow_on_which_neither_side_speaks_is_closed_with_its_own_detail() {
    let (_origin, destination) = origin().await;
    let (proxy, requests) = proxy(ip_allow(destination)).await;
    let (_guest, gateway_guest) = guest_pair().await;
    let (_, outcome) = proxy
        .handle_with(gateway_guest, destination, &CancellationToken::new(), dial)
        .await;
    assert_eq!(missing_sni_detail(&outcome), Some("server_first_timeout"));
    assert_eq!(outcome.metric_tags(), ("rejected", "tls_interception"));
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_destination_that_closes_before_speaking_is_closed_with_its_own_detail() {
    let (origin, destination) = origin().await;
    let (proxy, _) = proxy(ip_allow(destination)).await;
    let (_guest, gateway_guest) = guest_pair().await;
    let server = async {
        drop(origin.accept().await.unwrap());
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            server,
        )
    })
    .await
    .unwrap();
    assert_eq!(
        missing_sni_detail(&outcome),
        Some("server_first_upstream_closed")
    );
}

/// A domain allow needs the ClientHello to bind the hostname, so a silent
/// guest is still refused and the destination is never dialled.
#[tokio::test(start_paused = true)]
async fn a_dns_only_allow_still_fails_closed_for_a_silent_guest() {
    let policy = intercepted(NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain("pypi.org".parse().unwrap())],
            Vec::new(),
        )
        .unwrap(),
    });
    let (proxy, _) = proxy(policy.clone()).await;
    proxy
        .network_state
        .dns_allow_cache()
        .observe_response(&dns_a_response("pypi.org", [93, 184, 216, 34], 60), &policy);
    let destination: SocketAddr = "93.184.216.34:21".parse().unwrap();
    let TrafficPolicy::Ready(installed) = proxy.network_state.wait_for_tcp_policy().await else {
        panic!("test policy must be ready");
    };
    assert_eq!(
        proxy.missing_client_hello(destination, &TrafficPolicy::Ready(installed)),
        sniff::MissingClientHello::Denied
    );
    let (_guest, gateway_guest) = guest_pair().await;
    let (_, outcome) = proxy
        .handle_with(
            gateway_guest,
            destination,
            &CancellationToken::new(),
            must_not_dial,
        )
        .await;
    assert!(matches!(
        outcome,
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::DnsAttribution(
            DenyReason::Default
        )))
    ));
}

/// Without interception a silent guest is relayed after the short peek over
/// one destination connection; there is no probe.
#[tokio::test]
async fn interception_off_relays_a_silent_guest_without_probing() {
    let (origin, destination) = origin().await;
    let (proxy, _) = proxy(NetworkPolicyV1::allow_all()).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let server = async {
        let (mut stream, _) = origin.accept().await.unwrap();
        stream.write_all(b"220 ready\r\n").await.unwrap();
    };
    let client = async {
        let mut greeting = Vec::new();
        guest.read_to_end(&mut greeting).await.unwrap();
        assert_eq!(greeting, b"220 ready\r\n");
        guest.shutdown().await.unwrap();
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            server,
            client,
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        FlowOutcome::Relayed(Relay::Tunnelled { .. })
    ));
    assert!(origin.accept().now_or_never().is_none());
}

/// The guest wins when its ClientHello arrived before the greeting, even if
/// Tokio sees the destination first. The dialer delivers both while the probe
/// already waits on the guest, then blocks so the other worker's reactor marks
/// both sockets readable before the probe reads the greeting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_guest_that_spoke_before_the_greeting_wins_and_keeps_its_bytes() {
    use std::io::Write as _;

    let hello = client_hello(Some("good.example.test"));
    let (guest, gateway_guest) = guest_pair().await;
    let (origin, upstream) = guest_pair().await;
    let mut guest = guest.into_std().unwrap();
    let mut origin = origin.into_std().unwrap();
    let writers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let connect = {
        let (hello, writers) = (hello.clone(), Arc::clone(&writers));
        move |_| async move {
            guest.write_all(&hello).unwrap();
            origin.write_all(b"220 ready\r\n").unwrap();
            std::thread::sleep(Duration::from_millis(50));
            writers.lock().unwrap().extend([guest, origin]);
            Ok::<_, transport::Error>(upstream)
        }
    };
    let probe = tokio::time::timeout(
        Duration::from_secs(5),
        server_first::probe(
            &gateway_guest,
            "192.0.2.1:21".parse().unwrap(),
            connect,
            server_first::PROBE_WINDOW,
            &CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(probe, server_first::Probe::GuestFirst));
    let mut unread = vec![0_u8; hello.len()];
    let peeked = tokio::time::timeout(Duration::from_secs(5), gateway_guest.peek(&mut unread))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        &unread[..peeked],
        hello.as_slice(),
        "the probe consumed guest bytes"
    );
}

/// A policy update during the probe never lets the flow through on the old
/// policy: one that denies the destination cancels it through the resweep, and
/// any other refuses the handoff. Neither side receives a byte.
#[tokio::test]
async fn a_policy_update_during_the_probe_stops_the_flow() {
    for name in ["deny", "unrelated allow"] {
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let update = if name == "deny" {
            intercepted(NetworkPolicyV1::deny_all())
        } else {
            let mut policy = ip_allow(destination);
            policy.egress = EgressPolicyV1::new(
                EgressDefaultAction::Deny,
                vec![
                    EgressDestinationV1::Ip(destination.ip()),
                    EgressDestinationV1::Ip("198.51.100.7".parse().unwrap()),
                ],
                Vec::new(),
            )
            .unwrap();
            policy
        };
        let server = async {
            let (mut probe, _) = origin.accept().await.unwrap();
            install_policy(
                &proxy.network_state,
                update,
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .await;
            // A greeting the gateway must not forward; a cancelled probe has
            // already closed, so the write may fail.
            let _ = probe.write_all(b"220 ready\r\n").await;
            let mut received = Vec::new();
            let _ = probe.read_to_end(&mut received).await;
            assert!(
                received.is_empty(),
                "{name}: the destination received guest bytes"
            );
        };
        let client = async {
            // The gateway may reset the refused connection.
            let mut received = Vec::new();
            let _ = guest.read_to_end(&mut received).await;
            assert!(
                received.is_empty(),
                "{name}: the guest received the greeting"
            );
        };
        let shutdown = CancellationToken::new();
        let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                server,
                client,
            )
        })
        .await
        .expect(name);
        if name == "deny" {
            assert!(matches!(outcome, FlowOutcome::Cancelled), "{name}");
        } else {
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::PolicyChanged))
                ),
                "{name}"
            );
        }
        assert!(requests.lock().unwrap().is_empty(), "{name}");
    }
}
