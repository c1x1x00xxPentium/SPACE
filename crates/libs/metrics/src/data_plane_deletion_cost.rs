//! Scale-in ordering for the data plane: the cluster-singleton writer of
//! `controller.kubernetes.io/pod-deletion-cost` on data-plane pods.
//!
//! The writer probes each Ready data-plane pod's internal stream-load report,
//! buckets its count of attached streams, and patches the annotation
//! when the bucket changes. These series say whether the ordering signal is
//! current; they do not say whether a scale-in actually honored it.

use crate::emit::{counter, gauge, tag};

metric_vocabulary!(
    /// Outcome of reading one pod's stream-load report.
    ProbeOutcome {
        /// The report was read and parsed.
        Ok => "ok",
        /// The pod IP did not parse, or the request failed, returned a
        /// non-success status, or carried an unparseable body. The pod's
        /// annotation keeps its last value.
        Error => "error",
        /// The request did not finish within the probe timeout. The pod's
        /// annotation keeps its last value.
        Timeout => "timeout",
    }
);

metric_vocabulary!(
    /// What the writer did with one probed pod's annotation.
    WriteOutcome {
        /// The patch landed with a new bucket.
        Applied => "applied",
        /// The annotation already carried the desired bucket, or carried none
        /// while the desired bucket is `0` (an absent annotation already ranks
        /// as cost 0).
        Unchanged => "unchanged",
        /// The bucket changed, but this pod was written less than the minimum
        /// write interval ago; the write waits for a later tick.
        RateLimited => "rate_limited",
        /// The apiserver rejected the patch, or it timed out.
        Error => "error",
    }
);

metric_vocabulary!(
    /// The deletion-cost bucket a Ready data-plane pod falls in this tick.
    CostBucket {
        /// No attached streams: first to go on scale-in.
        Idle => "0",
        /// 1–9 attached streams.
        Low => "10",
        /// 10–99 attached streams.
        Medium => "100",
        /// 100 or more attached streams.
        High => "1000",
        /// The probe failed this tick, so the load is unknown and the pod
        /// keeps whatever annotation it had. Alert on this staying non-zero:
        /// the ordering signal for those pods is not being refreshed.
        Unknown => "unknown",
    }
);

pub fn probe(outcome: ProbeOutcome) {
    counter(
        "data_plane_deletion_cost.probe",
        1,
        &[tag("outcome", outcome)],
    );
}

/// `count` pods that shared a write outcome this tick, emitted as one packet.
pub fn writes(outcome: WriteOutcome, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "data_plane_deletion_cost.write",
        count,
        &[tag("outcome", outcome)],
    );
}

/// Ready data-plane pods in `bucket` this tick. Emitted for every bucket each
/// tick, zeros included, so a bucket that empties reads as zero rather than
/// holding its last value; the buckets sum to the Ready pods probed.
pub fn pods(bucket: CostBucket, count: usize) {
    gauge(
        "data_plane_deletion_cost.pods",
        count as f64,
        &[tag("cost", bucket)],
    );
}
