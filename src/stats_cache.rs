//! Coordinator-side cache of per-node BM25 term statistics.
//!
//! Every scoring share is bound to a 32-byte shard lifetime identity and its
//! mutation epoch. The coordinator echoes both on scoring requests, checked
//! under the same read guard as postings. A replacement at the same address
//! cannot reuse a cached share even if its counter happens to match.
//!
//! A stale claim invalidates the cache and retries once with freshly fetched
//! statistics and their complete version. A second concurrent change refuses;
//! retries never drop the fence. Missing identities from older nodes refuse.
//!
//! Shares are cached per node and validated document visibility. A response
//! must echo its view fingerprint before insertion, and a restricted view
//! cannot populate an unrestricted entry. Visibility is a data-view identity,
//! not a substitute for the caller's current authorization decision.
//!
//! Invalidation is per node and covers every visibility scope when its version
//! changes. A primary and replica have distinct lifetimes, even when they serve
//! the same persisted image. These versions are transient fencing identities,
//! not durable public document identities or cross-shard snapshot versions.

use crate::stats_identity::StatsClaim;
use crate::visibility::VisibilityScope;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tonic::Status;

/// One field's share of the stats on one node (the named-field channel
/// of `TermStatsRequest.fields`).
#[derive(Clone, Default)]
struct FieldShare {
    total_len: u64,
    known: bool,
    /// Whether the node's field carries token positions
    /// (docs/phrase-proximity.md); a store property like `known`.
    positions: bool,
    dfs: HashMap<String, u32>,
}

/// One node's cached share, valid at `epoch`.
struct NodeShare {
    epoch: StatsClaim,
    doc_count: u64,
    /// Body channel (`TermStatsRequest.terms`): the bare-terms share.
    /// Kept separate from a field literally named "body" because the
    /// wire keeps them separate; conflating them here would have the
    /// cache answer a shape the node was never asked.
    body_total_len: u64,
    body_dfs: HashMap<String, u32>,
    /// Named-field channel, by field name.
    fields: HashMap<String, FieldShare>,
    visibility_columns_known: Vec<bool>,
}

/// A body-channel lookup or fetch result for one node, everything a
/// query needs from that node: its share of the globals and the epoch
/// the share is valid at.
#[derive(Clone)]
pub struct BodyShare {
    pub epoch: StatsClaim,
    pub doc_count: u64,
    pub total_doc_length: u64,
    /// Per requested term, in request order.
    pub dfs: Vec<u32>,
    pub visibility_columns_known: Vec<bool>,
}

/// One field's slice of a fused-channel lookup, in request field order.
#[derive(Clone)]
pub struct FusedFieldShare {
    pub total_doc_length: u64,
    pub known: bool,
    /// Whether the field carries token positions on this node.
    pub positions: bool,
    /// Per requested term, in request order.
    pub dfs: Vec<u32>,
}

/// A fused-channel lookup or fetch result for one node.
#[derive(Clone)]
pub struct FusedShare {
    pub epoch: StatsClaim,
    pub doc_count: u64,
    /// Parallel to the requested fields.
    pub fields: Vec<FusedFieldShare>,
    pub visibility_columns_known: Vec<bool>,
}

/// Per-node term maps are bounded; on overflow the map is cleared and
/// rebuilt from live traffic. A reset costs one stats fan-out on the
/// next query; an unbounded map costs memory forever. 64Ki terms per
/// channel per node is far past any realistic working set of query
/// vocabulary.
const MAX_TERMS_PER_CHANNEL: usize = 64 * 1024;
/// Policy churn cannot leave an unbounded number of document views resident.
const MAX_SCOPES_PER_NODE: usize = 32;

/// The cache: bounded visibility scopes per node, in shard order.
pub struct StatsCache {
    nodes: Mutex<Vec<HashMap<VisibilityScope, NodeShare>>>,
    /// TermStats RPCs the coordinator actually issued. Written by the
    /// coordinator's fetch path, read by tests proving the hit path
    /// issues none.
    fetches: AtomicU64,
}

impl StatsCache {
    pub fn new(n_nodes: usize) -> Self {
        Self {
            nodes: Mutex::new((0..n_nodes).map(|_| HashMap::new()).collect()),
            fetches: AtomicU64::new(0),
        }
    }

    /// Body-channel lookup: `Some` only when EVERY requested term is
    /// cached for this node (a partial answer would force a fetch
    /// anyway, and the fetch replies with every term at once).
    pub fn lookup_body(&self, node: usize, terms: &[String]) -> Option<BodyShare> {
        self.lookup_body_scoped(node, terms, &VisibilityScope::default())
    }

    pub fn lookup_body_scoped(
        &self,
        node: usize,
        terms: &[String],
        scope: &VisibilityScope,
    ) -> Option<BodyShare> {
        let guard = self.nodes.lock().expect("stats cache lock poisoned");
        let share = guard.get(node)?.get(scope)?;
        let dfs = terms
            .iter()
            .map(|t| share.body_dfs.get(t).copied())
            .collect::<Option<Vec<u32>>>()?;
        Some(BodyShare {
            epoch: share.epoch,
            doc_count: share.doc_count,
            total_doc_length: share.body_total_len,
            dfs,
            visibility_columns_known: share.visibility_columns_known.clone(),
        })
    }

    /// Fused-channel lookup: `Some` only when every requested field and
    /// every term under it is cached for this node.
    pub fn lookup_fused(
        &self,
        node: usize,
        fields: &[crate::pb::FieldTerms],
    ) -> Option<FusedShare> {
        self.lookup_fused_scoped(node, fields, &VisibilityScope::default())
    }

    pub fn lookup_fused_scoped(
        &self,
        node: usize,
        fields: &[crate::pb::FieldTerms],
        scope: &VisibilityScope,
    ) -> Option<FusedShare> {
        let guard = self.nodes.lock().expect("stats cache lock poisoned");
        let share = guard.get(node)?.get(scope)?;
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
                positions: fs.positions,
                dfs,
            });
        }
        Some(FusedShare {
            epoch: share.epoch,
            doc_count: share.doc_count,
            fields: out,
            visibility_columns_known: share.visibility_columns_known.clone(),
        })
    }

    /// Record an unrestricted `TermStats` response. Same epoch and view:
    /// merge terms the cache lacked. A changed epoch evicts every view of
    /// this node. A malformed response leaves the cache unchanged.
    pub fn store(
        &self,
        node: usize,
        terms: &[String],
        fields: &[crate::pb::FieldTerms],
        resp: &crate::pb::TermStatsResponse,
    ) -> Result<(), Status> {
        self.store_scoped(node, terms, fields, &VisibilityScope::default(), resp)
    }

    /// Retain a validated share only under the view that requested it. A
    /// response missing the visibility echo cannot populate either cache.
    pub fn store_scoped(
        &self,
        node: usize,
        terms: &[String],
        fields: &[crate::pb::FieldTerms],
        scope: &VisibilityScope,
        resp: &crate::pb::TermStatsResponse,
    ) -> Result<(), Status> {
        crate::visibility::validate_stats_mode(false, resp)?;
        scope.validate_response(resp)?;
        let claim = StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?;
        if resp.stats_epoch == 0
            || resp.doc_frequencies.len() != terms.len()
            || resp.field_stats.len() != fields.len()
            || fields
                .iter()
                .zip(&resp.field_stats)
                .any(|(request, share)| request.terms.len() != share.doc_frequencies.len())
        {
            return Err(Status::failed_precondition(
                "term statistics cache requires an epoch and complete response shapes",
            ));
        }
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        let Some(slot) = guard.get_mut(node) else {
            return Ok(());
        };
        // A data mutation invalidates every view of that node. View churn is
        // bounded independently of the per-view term limits.
        if slot.values().any(|share| share.epoch != claim)
            || (!slot.contains_key(scope) && slot.len() >= MAX_SCOPES_PER_NODE)
        {
            slot.clear();
        }
        let share = slot.entry(scope.clone()).or_insert_with(|| NodeShare {
            epoch: claim,
            doc_count: 0,
            body_total_len: 0,
            body_dfs: HashMap::new(),
            fields: HashMap::new(),
            visibility_columns_known: resp.visibility_columns_known.clone(),
        });
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
            entry.positions = fs.positions;
            if entry.dfs.len() + ft.terms.len() > MAX_TERMS_PER_CHANNEL {
                entry.dfs.clear();
            }
            for (t, df) in ft.terms.iter().zip(&fs.doc_frequencies) {
                entry.dfs.insert(t.clone(), *df);
            }
        }
        Ok(())
    }

    /// Drop one node's share (a scoring request came back refused: the
    /// node's store moved past the cached epoch).
    pub fn invalidate(&self, node: usize) {
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        if let Some(slot) = guard.get_mut(node) {
            slot.clear();
        }
    }

    /// Drop every node's share.
    pub fn invalidate_all(&self) {
        let mut guard = self.nodes.lock().expect("stats cache lock poisoned");
        for slot in guard.iter_mut() {
            slot.clear();
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

#[cfg(test)]
mod scope_tests {
    use super::*;
    use crate::pb::{
        filter_expr, DocumentVisibility, FacetPredicate, FilterExpr, TermStatsResponse,
    };

    #[test]
    fn version_probes_cannot_replace_or_evict_corpus_statistics() {
        let cache = StatsCache::new(1);
        let scope = VisibilityScope::default();
        let response = TermStatsResponse {
            stats_epoch: 4,
            stats_incarnation: vec![1; 32],
            doc_count: 17,
            total_doc_length: 91,
            ..Default::default()
        };
        cache.store_scoped(0, &[], &[], &scope, &response).unwrap();
        let probe = TermStatsResponse {
            version_only: true,
            stats_epoch: 5,
            stats_incarnation: vec![2; 32],
            ..Default::default()
        };
        assert_eq!(
            cache
                .store_scoped(0, &[], &[], &scope, &probe)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let retained = cache.lookup_body_scoped(0, &[], &scope).unwrap();
        assert_eq!(retained.doc_count, 17);
        assert_eq!(retained.total_doc_length, 91);
        assert_eq!(retained.epoch, StatsClaim::required(4, &[1; 32]).unwrap());
    }

    #[test]
    fn replacing_a_lifetime_at_the_same_epoch_evicts_all_views() {
        let cache = StatsCache::new(1);
        let view = DocumentVisibility {
            filter: Some(FilterExpr {
                expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                    column: "tenant".into(),
                    values: vec!["one".into()],
                })),
            }),
        };
        let restricted = VisibilityScope::new(Some(&view)).unwrap();
        let scopes = [VisibilityScope::default(), restricted.clone()];
        for scope in &scopes {
            cache
                .store_scoped(
                    0,
                    &[],
                    &[],
                    scope,
                    &TermStatsResponse {
                        version_only: false,
                        stats_epoch: 3,
                        stats_incarnation: vec![1; 32],
                        doc_count: 2,
                        visibility_fingerprint: scope.fingerprint().to_vec(),
                        visibility_columns_known: vec![true; scope.column_count()],
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        let mut next = TermStatsResponse {
            version_only: false,
            stats_epoch: 3,
            stats_incarnation: vec![],
            doc_count: 7,
            ..Default::default()
        };
        assert!(cache.store(0, &[], &[], &next).is_err());
        assert_eq!(cache.lookup_body(0, &[]).unwrap().doc_count, 2);
        next.stats_incarnation = vec![2; 32];
        cache.store(0, &[], &[], &next).unwrap();
        assert_eq!(cache.lookup_body(0, &[]).unwrap().doc_count, 7);
        assert!(cache.lookup_body_scoped(0, &[], &restricted).is_none());
    }

    #[test]
    fn policy_churn_is_bounded_and_bad_shapes_do_not_poison_existing_shares() {
        let cache = StatsCache::new(1);
        for n in 0..1000 {
            let view = DocumentVisibility {
                filter: Some(FilterExpr {
                    expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                        column: "tenant".into(),
                        values: vec![n.to_string()],
                    })),
                }),
            };
            let scope = VisibilityScope::new(Some(&view)).unwrap();
            let response = TermStatsResponse {
                version_only: false,
                stats_epoch: 1,
                stats_incarnation: vec![1; 32],
                doc_count: n,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: vec![true],
                ..Default::default()
            };
            cache.store_scoped(0, &[], &[], &scope, &response).unwrap();
            assert!(cache.nodes.lock().unwrap()[0].len() <= MAX_SCOPES_PER_NODE);
            assert_eq!(
                cache.lookup_body_scoped(0, &[], &scope).unwrap().doc_count,
                n
            );
            let mut malformed = response;
            malformed.doc_frequencies.push(1);
            assert!(cache.store_scoped(0, &[], &[], &scope, &malformed).is_err());
            assert_eq!(
                cache.lookup_body_scoped(0, &[], &scope).unwrap().doc_count,
                n
            );
        }
        cache.invalidate(0);
        assert!(cache.nodes.lock().unwrap()[0].is_empty());
    }
}
