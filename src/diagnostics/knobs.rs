//! Runtime knobs: their static descriptions, the live atomics a process
//! reads at request time, and the read-at-startup list beside them.

use super::*;

/// The ring keeps this many recent requests.
pub const RECENT_RING: usize = 256;
/// `RecentQueriesRequest.limit == 0` selects this many.
pub const RECENT_DEFAULT: usize = 50;
/// `StreamMetricsRequest.interval_ms == 0` selects this.
pub const STREAM_DEFAULT_MS: u32 = 1000;
/// Below this the stream is rejected.
pub const STREAM_MIN_MS: u32 = 100;

/// One knob's static description.
pub(super) struct KnobSpec {
    pub(super) name: &'static str,
    pub(super) scope: KnobScope,
    pub(super) kind: KnobKind,
    pub(super) description: &'static str,
}

pub(super) const FLOOR_SHARING: KnobSpec = KnobSpec {
    name: "floor_sharing",
    scope: KnobScope::Node,
    kind: KnobKind::Bool,
    description: "Publish this shard's k-th best score to the coordinator and prune \
                  candidates under the shared cutoff (--floor-sharing).",
};
pub(super) const SEGMENT_PRUNING: KnobSpec = KnobSpec {
    name: "segment_pruning",
    scope: KnobScope::Node,
    kind: KnobKind::Bool,
    description: "Skip sealed segments whose column summary rules out the request's filter \
                  (--segment-pruning).",
};
pub(super) const FLOOR_DELTA: KnobSpec = KnobSpec {
    name: "floor_delta",
    scope: KnobScope::Node,
    kind: KnobKind::Float,
    description: "Smallest score movement that publishes a new floor (--floor-delta).",
};
pub(super) const FLOOR_WARMUP_CHUNKS: KnobSpec = KnobSpec {
    name: "floor_warmup_chunks",
    scope: KnobScope::Node,
    kind: KnobKind::Int,
    description: "Scan chunks to finish before the first floor is published \
                  (--floor-warmup-chunks).",
};
pub(super) const FLOOR_MIN_INTERVAL_MS: KnobSpec = KnobSpec {
    name: "floor_min_interval_ms",
    scope: KnobScope::Node,
    kind: KnobKind::Int,
    description: "Shortest gap between floor publications in milliseconds; 0 publishes on \
                  each movement (--floor-min-interval-ms).",
};
pub(super) const MAX_K: KnobSpec = KnobSpec {
    name: "max_k",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Largest k a request may ask for, and the depth an omitted k runs at \
                  (--max-k).",
};
pub(super) const HEDGE_DELAY_MS: KnobSpec = KnobSpec {
    name: "hedge_delay_ms",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Wait on a shard's primary before racing its replica; 0 disables hedging \
                  (--hedge-delay-ms).",
};
pub(super) const SIGNAL_BATCH: KnobSpec = KnobSpec {
    name: "signal_batch",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Int,
    description: "Candidate ids per rescore call (Bm25Rescore, VectorRescore) when a boolean \
                  group scores a clause over its surviving ids; the pieces multiply per-call \
                  cost, and max_k pieces are the earlier behavior (--signal-batch).",
};
pub(super) const SHARD_PRUNING: KnobSpec = KnobSpec {
    name: "shard_pruning",
    scope: KnobScope::Coordinator,
    kind: KnobKind::Bool,
    description: "Skip shards whose placement leaf rules out the request's filter before \
                  fan-out (--shard-pruning, docs/placement.md).",
};

/// Live values of a node's knobs, taken once from its config.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeKnobValues {
    pub share_floors: bool,
    pub segment_pruning: bool,
    pub floor_delta: f32,
    pub floor_warmup_chunks: u32,
    pub floor_min_interval_ms: u64,
}

/// Live values of a coordinator's knobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorKnobValues {
    pub max_k: u32,
    /// 0 means no hedging.
    pub hedge_delay_ms: u64,
    pub shard_pruning: bool,
    /// Candidate ids per rescore call; at least 1.
    pub signal_batch: u32,
}

pub(super) enum Live {
    Node {
        share_floors: AtomicBool,
        segment_pruning: AtomicBool,
        floor_delta: AtomicU32,
        floor_warmup_chunks: AtomicU32,
        floor_min_interval_ms: AtomicU64,
        startup: NodeKnobValues,
    },
    Coordinator {
        max_k: AtomicU32,
        hedge_delay_ms: AtomicU64,
        shard_pruning: AtomicBool,
        signal_batch: AtomicU32,
        startup: CoordinatorKnobValues,
    },
}

/// A process's runtime knobs: the mutable ones as atomics, the rest as
/// the text they were configured with.
pub struct Knobs {
    pub(super) process: String,
    pub(super) live: Live,
    pub(super) fixed: std::sync::RwLock<Vec<RuntimeKnob>>,
}

impl std::fmt::Debug for Knobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Knobs")
            .field("process", &self.process)
            .field("knobs", &self.list().knobs.len())
            .finish()
    }
}

/// One immutable setting to list beside the live ones.
pub struct FixedKnob {
    pub name: &'static str,
    pub kind: KnobKind,
    pub value: String,
    pub description: &'static str,
}

pub(super) fn fixed_entry(scope: KnobScope, fixed: FixedKnob) -> RuntimeKnob {
    RuntimeKnob {
        name: fixed.name.to_string(),
        scope: scope as i32,
        kind: fixed.kind as i32,
        value: fixed.value.clone(),
        startup_value: fixed.value,
        mutable: false,
        description: fixed.description.to_string(),
    }
}

impl Knobs {
    /// A node's knobs from its construction-time values.
    pub fn node(process: impl Into<String>, values: NodeKnobValues, fixed: Vec<FixedKnob>) -> Self {
        Knobs {
            process: process.into(),
            live: Live::Node {
                share_floors: AtomicBool::new(values.share_floors),
                segment_pruning: AtomicBool::new(values.segment_pruning),
                floor_delta: AtomicU32::new(values.floor_delta.to_bits()),
                floor_warmup_chunks: AtomicU32::new(values.floor_warmup_chunks),
                floor_min_interval_ms: AtomicU64::new(values.floor_min_interval_ms),
                startup: values,
            },
            fixed: std::sync::RwLock::new(
                fixed
                    .into_iter()
                    .map(|f| fixed_entry(KnobScope::Node, f))
                    .collect(),
            ),
        }
    }

    /// A coordinator's knobs.
    pub fn coordinator(
        process: impl Into<String>,
        values: CoordinatorKnobValues,
        fixed: Vec<FixedKnob>,
    ) -> Self {
        Knobs {
            process: process.into(),
            live: Live::Coordinator {
                max_k: AtomicU32::new(values.max_k),
                hedge_delay_ms: AtomicU64::new(values.hedge_delay_ms),
                shard_pruning: AtomicBool::new(values.shard_pruning),
                signal_batch: AtomicU32::new(values.signal_batch),
                startup: values,
            },
            fixed: std::sync::RwLock::new(
                fixed
                    .into_iter()
                    .map(|f| fixed_entry(KnobScope::Coordinator, f))
                    .collect(),
            ),
        }
    }

    /// Replace the read-at-startup list (a coordinator's builders call
    /// this as they settle its configuration).
    pub fn set_fixed(&self, fixed: Vec<FixedKnob>) {
        let scope = match &self.live {
            Live::Node { .. } => KnobScope::Node,
            Live::Coordinator { .. } => KnobScope::Coordinator,
        };
        *self.fixed.write().expect("knob list lock poisoned") =
            fixed.into_iter().map(|f| fixed_entry(scope, f)).collect();
    }

    pub fn process(&self) -> &str {
        &self.process
    }

    // -- node reads -------------------------------------------------------

    pub fn share_floors(&self) -> bool {
        match &self.live {
            Live::Node { share_floors, .. } => share_floors.load(Ordering::Relaxed),
            Live::Coordinator { .. } => true,
        }
    }

    pub fn segment_pruning(&self) -> bool {
        match &self.live {
            Live::Node {
                segment_pruning, ..
            } => segment_pruning.load(Ordering::Relaxed),
            Live::Coordinator { .. } => true,
        }
    }

    /// Coordinator: whether a filter skips shards from their placement
    /// leaf. A node answers `true`; it holds no
    /// topology.
    pub fn shard_pruning(&self) -> bool {
        match &self.live {
            Live::Coordinator { shard_pruning, .. } => shard_pruning.load(Ordering::Relaxed),
            Live::Node { .. } => true,
        }
    }

    pub fn floor_delta(&self) -> f32 {
        match &self.live {
            Live::Node { floor_delta, .. } => f32::from_bits(floor_delta.load(Ordering::Relaxed)),
            Live::Coordinator { .. } => 0.0,
        }
    }

    pub fn floor_warmup_chunks(&self) -> u32 {
        match &self.live {
            Live::Node {
                floor_warmup_chunks,
                ..
            } => floor_warmup_chunks.load(Ordering::Relaxed),
            Live::Coordinator { .. } => 0,
        }
    }

    pub fn floor_min_interval_ms(&self) -> u64 {
        match &self.live {
            Live::Node {
                floor_min_interval_ms,
                ..
            } => floor_min_interval_ms.load(Ordering::Relaxed),
            Live::Coordinator { .. } => 0,
        }
    }

    // -- coordinator reads ------------------------------------------------

    pub fn max_k(&self) -> u32 {
        match &self.live {
            Live::Coordinator { max_k, .. } => max_k.load(Ordering::Relaxed),
            Live::Node { .. } => 0,
        }
    }

    /// Candidate ids per rescore call (coordinator only; 0 on a node,
    /// which has no such knob).
    pub fn signal_batch(&self) -> u32 {
        match &self.live {
            Live::Coordinator { signal_batch, .. } => signal_batch.load(Ordering::Relaxed),
            Live::Node { .. } => 0,
        }
    }

    pub fn hedge_delay(&self) -> Option<Duration> {
        match &self.live {
            Live::Coordinator { hedge_delay_ms, .. } => {
                let ms = hedge_delay_ms.load(Ordering::Relaxed);
                (ms > 0).then(|| Duration::from_millis(ms))
            }
            Live::Node { .. } => None,
        }
    }
}
