//! Wire contract for the data plane's internal stream-load report.
//!
//! The data plane serves this on an internal listener that is not part of its
//! public Service; the cluster controller reads it to order data-plane scale-in
//! (see `controller.kubernetes.io/pod-deletion-cost`). Both sides take the
//! path and the body shape from here so they cannot drift; the port is
//! whatever the pod declares under [`DATA_PLANE_INTERNAL_PORT_NAME`].

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Path of the stream-load report on the data plane's internal listener.
pub const DATA_PLANE_STREAM_LOAD_PATH: &str = "/internal/v1/stream-load";

/// Name of the container port that declares the internal listener. The cluster
/// controller probes the pod on whichever port carries this name, so the
/// deployment that sets `--internal-listen` also tells the reader where it is.
pub const DATA_PLANE_INTERNAL_PORT_NAME: &str = "internal";

/// One data-plane replica's current stream load.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataPlaneStreamLoad {
    /// Every stream attached to this replica right now, whatever its age:
    /// what a scale-in that removes this replica would cut.
    pub active_streams: u64,
}
