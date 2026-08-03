//! Coordinator-side cache of per-node BM25 term statistics.
//!
//! Every BM25-scoring query needs the GLOBAL corpus stats for its terms
//! (docs/multi-field.md), which the coordinator assembles by summing
//! each node's share. Those shares are a pure function of a node's BM25
//! store, and the store advertises a `stats_epoch` that advances on
//! every mutation — so a share fetched at epoch E may be reused for as
//! long as the node still reports E, and the node itself enforces the
//! claim: scoring requests echo the epoch back as
//! `expected_stats_epoch`, and a shard whose store moved on REFUSES
//! rather than scoring with stats that no longer describe it. The
//! cache is therefore never a source of silent staleness; at worst it
//! costs one refused round trip, after which the coordinator refetches
//! and repeats the query under today's uncached semantics.
//!
//! Shares are cached PER NODE rather than merged, because invalidation
//! is per node: one shard taking an ingest batch must not evict seven
//! other shards' shares. Epochs are process-local counters and are
//! only ever compared against the same node they came from; in
//! particular they are meaningless across a primary/replica pair, which
//! is safe today because no BM25 request is hedged to a replica.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// One field's share of the stats on one node (the named-field channel
/// of `TermStatsRequest.fields`).
#[derive(Clone, Default)]
struct FieldShare {
    total_len: u64,
    known: bool,
    dfs: HashMap<String, u32>,
}

/// One node's cached share, valid at `epoch`.
struct NodeShare {
    epoch: u64,
    doc_count: u64,
    /// Body channel (`TermStatsRequest.terms`): the bare-terms share.
    /// Kept separate from a field literally named "body" because the
    /// wire keeps them separate; conflating them here would have the
    /// cache answer a shape the node was never asked.
    body_total_len: u64,
    body_dfs: HashMap<String, u32>,
    /// Named-field channel, by field name.
    fields: HashMap<String, FieldShare>,
}

/// A body-channel lookup or fetch result for one node, everything a
/// query needs from that node: its share of the globals and the epoch
/// the share is valid at.
#[derive(Clone)]
pub struct BodyShare {
    pub epoch: u64,
    pub doc_count: u64,
    pub total_doc_length: u64,
    /// Per requested term, in request order.
    pub dfs: Vec<u32>,
}

/// One field's slice of a fused-channel lookup, in request field order.
#[derive(Clone)]
pub struct FusedFieldShare {
    pub total_doc_length: u64,
    pub known: bool,
    /// Per requested term, in request order.
    pub dfs: Vec<u32>,
}

/// A fused-channel lookup or fetch result for one node.
#[derive(Clone)]
pub struct FusedShare {
    pub epoch: u64,
    pub doc_count: u64,
    /// Parallel to the requested fields.
    pub fields: Vec<FusedFieldShare>,
}

/// Per-node term maps are bounded; on overflow the map is cleared and
/// rebuilt from live traffic. A reset costs one stats fan-out on the
/// next query; an unbounded map costs memory forever. 64Ki terms per
/// channel per node is far past any realistic working set of query
/// vocabulary.
const MAX_TERMS_PER_CHANNEL: usize = 64 * 1024;

/// The cache: one optional share per node, in shard order.
pub struct StatsCache {
    nodes: Mutex<Vec<Option<NodeShare>>>,
    /// TermStats RPCs the coordinator actually issued. Written by the
    /// coordinator's fetch path, read by tests proving the hit path
    /// issues none.
    fetches: AtomicU64,
}

impl StatsCache {
    pub fn new(n_nodes: usize) -> Self {
        Self {
            nodes: Mutex::new((0..n_nodes).map(|_| None).collect()),
            fetches: AtomicU64::new(0),
        }
    }

    /// Body-channel lookup: `Some` only when EVERY requested term is
    /// cached for this node (a partial answer would force a fetch
    /// anyway, and the fetch replies with every term at once).
    pub fn lookup_body(&self, node: usize, terms: &[String]) -> Option<BodyShare> {
        let guard = self.nodes.lock().expect("stats cache lock poisoned");
        let share = guard.get(node)?.as_ref()?;
        let dfs = terms
            .iter()
            .map(|t| share.body_dfs.get(t).copied())
            .collect::<Option<Vec<u32>>>()?;
        Some(BodyShare {
            epoch: share.epoch,
            doc_count: share.doc_count,
            total_doc_length: share.body_total_len,
            dfs,
        })
    }

    /// Fused-channel lookup: `Some` only when every requested field and
    /// every term under it is cached for this node.
    pub fn lookup_fused(&self, node: usize, fields: &[crate::pb::FieldTerms]) -> Option<FusedShare> {
        let guard = self.nodes.lock().expect("stats cache lock poisoned");
        let share = guard.get(node)?.as_ref()?;
        let mut out = Vec::with_capacity(fields.len());
        for ft in fields {
            let fs = share.fields.get(&ft.field)?;
            let dfs = ft
                .terms
                .iter()
                .map(|t| fs.dfs.get(t).copied())
                .collect::<Option<Vec<u32>>>()?;
            out.push(FusedFieldShare {
                total_doc_length: fs.total_len,
                known: fs.known,
                dfs,
            });
        }
        Some(FusedShare {
            epoch: share.epoch,
            doc_count: share.doc_count,
            fields: out,
        })
    }

    /// Record a node's `TermStats` response. Same epoch as cached:
    /// merge (the response answers terms the cache lacked). Different
    /// epoch: everything cached is stale, replace wholesale with just
    /// this response's terms.
    pub fn store(
        &self,
        node: usize,
        terms: &[String],
        fields: &[crate::pb::FieldTerms],
        resp: &crate::pb::TermStatsResponse,
    ) {
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        let Some(slot) = guard.get_mut(node) else {
            return;
        };
        let share = match slot {
            Some(s) if s.epoch == resp.stats_epoch => s,
            _ => {
                *slot = Some(NodeShare {
                    epoch: resp.stats_epoch,
                    doc_count: 0,
                    body_total_len: 0,
                    body_dfs: HashMap::new(),
                    fields: HashMap::new(),
                });
                slot.as_mut().expect("just written")
            }
        };
        share.doc_count = resp.doc_count;
        share.body_total_len = resp.total_doc_length;
        if share.body_dfs.len() + terms.len() > MAX_TERMS_PER_CHANNEL {
            share.body_dfs.clear();
        }
        for (t, df) in terms.iter().zip(&resp.doc_frequencies) {
            share.body_dfs.insert(t.clone(), *df);
        }
        for (ft, fs) in fields.iter().zip(&resp.field_stats) {
            let entry = share.fields.entry(ft.field.clone()).or_default();
            entry.total_len = fs.total_doc_length;
            entry.known = fs.known;
            if entry.dfs.len() + ft.terms.len() > MAX_TERMS_PER_CHANNEL {
                entry.dfs.clear();
            }
            for (t, df) in ft.terms.iter().zip(&fs.doc_frequencies) {
                entry.dfs.insert(t.clone(), *df);
            }
        }
    }

    /// Drop one node's share (a scoring request came back refused: the
    /// node's store moved past the cached epoch).
    pub fn invalidate(&self, node: usize) {
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        if let Some(slot) = guard.get_mut(node) {
            *slot = None;
        }
    }

    /// Drop every node's share.
    pub fn invalidate_all(&self) {
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        for slot in guard.iter_mut() {
            *slot = None;
        }
    }

    /// Count one issued TermStats RPC (called by the coordinator's
    /// fetch path).
    pub fn note_fetch(&self) {
        self.fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// TermStats RPCs issued so far. A repeated query leaving this
    /// unchanged IS the cache working, and is what the tests assert.
    pub fn fetch_count(&self) -> u64 {
        self.fetches.load(Ordering::Relaxed)
    }
}
