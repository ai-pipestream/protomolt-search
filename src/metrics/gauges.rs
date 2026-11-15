//! Gauges sampled at scrape time from live state.

/// One shard's gauges, sampled at SCRAPE time from live state, so a
/// gauge can never go stale and never needs an update site.
#[derive(Debug, Clone)]
pub struct ShardGauges {
    /// The shard's identity label value: its slot offset, the shard's
    /// name in the global id space.
    pub slot_offset: u64,
    /// Vectors in the shard's index.
    pub vectors: u64,
    /// Documents in the shard's postings.
    pub documents: u64,
    /// The BM25 statistics epoch (advances on every mutation).
    pub stats_epoch: u64,
}

/// A live-state gauge sampler for one shard. Returns VALUES rather
/// than rendering text so [`super::render()`] can group all shards' samples of
/// one metric under a single `# TYPE` header, as the exposition
/// format requires.
pub type GaugeProvider = Box<dyn Fn() -> ShardGauges + Send + Sync>;

/// One peer's rejections, sampled at SCRAPE time: the `count` this
/// peer has answered since the member started, labeled by the RPC and
/// the status code so the page and the snapshot carry what the status
/// route serves.
#[derive(Debug, Clone, Default)]
pub struct PeerRejectionGauges {
    /// The peer that answered the rejections.
    pub peer: u64,
    /// The RPC rejected: "append", "vote" or "snapshot".
    pub action: String,
    /// The status code, Debug name ("DataLoss").
    pub code: String,
    /// Rejections answered by this peer since the member started.
    pub count: u64,
}

/// One Raft member's gauges, sampled at SCRAPE time from the live host,
/// so a gauge can never go stale and never needs an update site. A
/// process serves at most one member, so the scalars carry no labels;
/// peer rejections are labeled by peer, RPC and code.
#[derive(Debug, Clone, Default)]
pub struct MemberGauges {
    /// Whether the member leads.
    pub is_leader: bool,
    /// Last log index applied to the state machine.
    pub applied_index: u64,
    /// Last log index appended to the log.
    pub last_log_index: u64,
    /// Whether the store awaits its first snapshot.
    pub awaiting_snapshot: bool,
    /// Peer rejections answered, one entry per peer.
    pub peer_rejections: Vec<PeerRejectionGauges>,
    /// Snapshot installs rejected at the announce.
    pub snapshot_rejections: u64,
    /// Bytes of the last snapshot build.
    pub last_snapshot_build_bytes: u64,
    /// Receiver bound of the last snapshot build.
    pub last_snapshot_receiver_bound_bytes: u64,
}

/// A live-state gauge sampler for the Raft member. Returns VALUES
/// rather than rendering text, as [`GaugeProvider`] does.
pub type MemberGaugeProvider = Box<dyn Fn() -> MemberGauges + Send + Sync>;

/// One per-shard gauge series: name, help text, and the value read from
/// one shard's sample.
type ShardGaugeSeries = (&'static str, &'static str, fn(&ShardGauges) -> u64);

/// The per-shard gauge series in page order.
pub(super) const SHARD_GAUGES: [ShardGaugeSeries; 3] = [
    (
        "turbovec_shard_vectors",
        "Vectors in the shard's index.",
        |s| s.vectors,
    ),
    (
        "turbovec_shard_documents",
        "Documents in the shard's postings.",
        |s| s.documents,
    ),
    (
        "turbovec_shard_stats_epoch",
        "BM25 statistics epoch (advances on every mutation).",
        |s| s.stats_epoch,
    ),
];

impl MemberGauges {
    /// The member's unlabeled series in page order: name, kind, help text,
    /// and value.
    pub(super) fn scalars(&self) -> [(&'static str, &'static str, &'static str, u64); 7] {
        [
            (
                "raft_is_leader",
                "gauge",
                "Whether this process's Raft member leads (1) or follows (0).",
                u64::from(self.is_leader),
            ),
            (
                "raft_applied_index",
                "gauge",
                "Last Raft log index applied to the state machine.",
                self.applied_index,
            ),
            (
                "raft_last_log_index",
                "gauge",
                "Last Raft log index appended to the log.",
                self.last_log_index,
            ),
            (
                "raft_awaiting_snapshot",
                "gauge",
                "Whether the store awaits its first snapshot (1) or not (0).",
                u64::from(self.awaiting_snapshot),
            ),
            (
                "raft_snapshot_rejections_total",
                "counter",
                "Snapshot installs rejected at the announce.",
                self.snapshot_rejections,
            ),
            (
                "raft_last_snapshot_build_bytes",
                "gauge",
                "Bytes of the last snapshot build.",
                self.last_snapshot_build_bytes,
            ),
            (
                "raft_last_snapshot_receiver_bound_bytes",
                "gauge",
                "Receiver bound of the last snapshot build.",
                self.last_snapshot_receiver_bound_bytes,
            ),
        ]
    }
}
