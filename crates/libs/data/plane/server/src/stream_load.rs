//! Per-replica stream load, served on an internal listener for the cluster
//! controller's scale-in ordering.
//!
//! The report is read-only and carries one aggregate count, nothing scoped to
//! an org, sandbox, or session. It is deliberately unauthenticated: the only
//! caller is the cluster controller dialing the pod IP over plain HTTP, and
//! sending `SPACE_INTERNAL_TOKEN` there would expose a credential that
//! authorizes node-api calls in exchange for protecting one integer. Keep the
//! listener off the public Service, and keep anything sensitive off this
//! router.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use pplx_space_common_core::data_plane_load::{DATA_PLANE_STREAM_LOAD_PATH, DataPlaneStreamLoad};
use tokio_util::sync::CancellationToken;

use crate::state::AttachedStreamLimiter;

pub(crate) fn stream_load(limiter: &AttachedStreamLimiter) -> DataPlaneStreamLoad {
    DataPlaneStreamLoad {
        active_streams: limiter.active_total() as u64,
    }
}

pub(crate) fn internal_router(limiter: std::sync::Arc<AttachedStreamLimiter>) -> Router {
    Router::new()
        .route(DATA_PLANE_STREAM_LOAD_PATH, get(stream_load_handler))
        .with_state(limiter)
}

async fn stream_load_handler(
    State(limiter): State<std::sync::Arc<AttachedStreamLimiter>>,
) -> Json<DataPlaneStreamLoad> {
    Json(stream_load(&limiter))
}

/// Serve the internal router on its own task until `shutdown` fires.
///
/// The listener's lifetime is tied to the process: if the serve task ends while
/// `shutdown` is still live, the supervisor cancels `shutdown` (so the public
/// listener drains and the process exits) and returns an error for `run` to
/// surface. A replica that kept serving without its load report would have its
/// ordering signal frozen and then guessed by the cluster controller.
///
/// With graceful shutdown, `axum::serve` neither errors nor returns before the
/// signal fires (it retries failed accepts), so in practice the early end this
/// guards against is a panic in the serve task. The supervisor watches the
/// task's `JoinHandle` rather than the serve result so a panic is caught too.
pub(crate) fn spawn_internal(
    listener: tokio::net::TcpListener,
    limiter: std::sync::Arc<AttachedStreamLimiter>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let stopped = shutdown.clone();
    let serve = tokio::spawn(async move {
        axum::serve(listener, internal_router(limiter))
            .with_graceful_shutdown(async move { stopped.cancelled().await })
            .await
    });
    tokio::spawn(supervise_internal(serve, shutdown))
}

/// Turn the serve task's end into the internal listener's outcome. Only a clean
/// stop after `shutdown` fired is `Ok`; anything else cancels `shutdown`.
pub(crate) async fn supervise_internal(
    serve: tokio::task::JoinHandle<std::io::Result<()>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let ended = serve.await;
    if shutdown.is_cancelled()
        && let Ok(Ok(())) = ended
    {
        return Ok(());
    }
    let error = match ended {
        Ok(Ok(())) => {
            std::io::Error::other("internal stream-load listener stopped before shutdown")
        }
        Ok(Err(error)) => error,
        Err(join) => std::io::Error::other(format!("internal stream-load listener task: {join}")),
    };
    tracing::error!(
        %error,
        "data-plane internal stream-load listener ended; shutting the replica down"
    );
    shutdown.cancel();
    Err(error)
}

#[cfg(test)]
mod tests;
