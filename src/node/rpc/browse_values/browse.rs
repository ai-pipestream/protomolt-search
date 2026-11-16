//! Filter-only shard browse: the id-ordered walk and the shared response echo.

use super::sorted::sorted_browse;
use crate::node::*;

/// Request-scoped facts every BrowseShard response echoes.
pub(super) struct BrowseEcho<'a> {
    pub(super) guard: &'a ShardState,
    pub(super) scope: &'a crate::visibility::VisibilityScope,
    pub(super) visibility_columns_known: Vec<bool>,
    pub(super) geo_columns_known: Vec<bool>,
    pub(super) filter_columns_known: Vec<bool>,
}

/// The part of a BrowseShard response that differs between its exits.
#[derive(Default)]
pub(super) struct BrowsePage {
    pub(super) prune: crate::segment_prune::PruneStats,
    pub(super) doc_ids: Vec<u64>,
    pub(super) sort_rows: Vec<crate::pb::SortKeyRow>,
    pub(super) sort_columns_known: Vec<bool>,
    pub(super) sort_column_types: Vec<i32>,
}

impl BrowseEcho<'_> {
    pub(super) fn respond(
        self,
        page: BrowsePage,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        Ok(Response::new(crate::pb::BrowseShardResponse {
            sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
            stats_epoch: self.guard.stats_epoch,
            stats_incarnation: self.guard.stats_incarnation.bytes()?,
            visibility_fingerprint: self.scope.fingerprint().to_vec(),
            visibility_columns_known: self.visibility_columns_known,
            segments_total: page.prune.segments_total,
            segments_skipped: page.prune.segments_skipped,
            doc_ids: page.doc_ids,
            geo_columns_known: self.geo_columns_known,
            filter_columns_known: self.filter_columns_known,
            sort_rows: page.sort_rows,
            sort_columns_known: page.sort_columns_known,
            sort_column_types: page.sort_column_types,
        }))
    }
}

/// The lexical membership predicate (the terms of one lexical leaf, OR): the
/// same bitmap ResolveLexicalBitmap answers, built here so the walk reads it
/// in place. `None` admits every document.
struct LexicalMembership(Option<Vec<u8>>);

impl LexicalMembership {
    fn build(store: &Bm25Shard, terms: &[String], n: u64) -> Result<Self, Status> {
        if terms.is_empty() {
            return Ok(Self(None));
        }
        let index = store.as_index();
        let count = n as usize;
        let mut bits = vec![0u8; count.div_ceil(8)];
        for term in terms {
            index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                let position = doc_id as usize;
                if position < count {
                    bits[position / 8] |= 1 << (position % 8);
                }
            });
        }
        Ok(Self(Some(bits)))
    }

    fn contains(&self, local: u32) -> bool {
        match &self.0 {
            None => true,
            Some(bits) => {
                let p = local as usize;
                bits.get(p / 8).is_some_and(|b| b & (1 << (p % 8)) != 0)
            }
        }
    }
}

/// The admitted document space one browse walks.
pub(super) struct BrowseWalk<'a> {
    pub(super) req: &'a crate::pb::BrowseShardRequest,
    pub(super) store: &'a Bm25Shard,
    pub(super) slot_offset: u64,
    /// Document slots on this shard.
    pub(super) n: u64,
    lexical: LexicalMembership,
    doc_filter: crate::filter::DocFilter<'a>,
    pub(super) prune: SegmentPrune,
}

impl BrowseWalk<'_> {
    pub(super) fn admits(&self, local: u32) -> bool {
        self.lexical.contains(local) && self.doc_filter.passes(local, &ShardNumericRead(self.store))
    }

    /// Id-ordered browse: admitted ids above the floor, ascending, at most k.
    fn id_ordered(&self) -> Vec<u64> {
        // The exclusive floor in local id space. The first page carries
        // no floor at all (proto3 cannot distinguish after = 0 from
        // unset, so the request says which it is).
        let start = if self.req.first_page || self.req.after < self.slot_offset {
            0
        } else {
            (self.req.after - self.slot_offset + 1).min(self.n)
        };
        let mut doc_ids = Vec::new();
        for local in self.prune.admitted_slots_from(start as u32, self.n as u32) {
            if self.admits(local) {
                doc_ids.push(self.slot_offset + u64::from(local));
                if doc_ids.len() == self.req.k as usize {
                    break;
                }
            }
        }
        doc_ids
    }
}

/// The declared value type of a map sort column on a document-less shard.
fn configured_map_sort_type(config: &NodeConfig, sort: &crate::pb::BrowseSort) -> i32 {
    use crate::pb::ScalarValueType as Type;
    let Some(map) = &sort.map else {
        return Type::Unspecified as i32;
    };
    let name = &map.column;
    (if config.map_integer_fields.contains(name) {
        Type::Integer
    } else if config.map_unsigned_integer_fields.contains(name) {
        Type::UnsignedInteger
    } else if config.map_numeric_fields.contains(name) {
        Type::Number
    } else if config.map_facet_fields.contains(name) {
        Type::Text
    } else {
        Type::Unspecified
    }) as i32
}

impl NodeServiceImpl {
    fn browse_walk<'a>(
        &self,
        req: &'a crate::pb::BrowseShardRequest,
        guard: &'a ShardState,
        store: &'a Bm25Shard,
        filter: Option<&crate::pb::FilterExpr>,
        geo_regions: &[crate::geo::GeoRegion],
    ) -> Result<BrowseWalk<'a>, Status> {
        let n = u64::from(store.next_doc_id());
        let lexical = LexicalMembership::build(store, &req.lexical_terms, n)?;
        let doc_filter = crate::filter::DocFilter {
            deleted: guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, geo_regions),
            pred: filter.map(|f| store.resolve_filter(f)).transpose()?,
            phrase: Vec::new(),
        };
        let prune = prune_segments(
            store,
            doc_filter.pred.as_ref(),
            self.knobs.segment_pruning(),
        );
        Ok(BrowseWalk {
            req,
            store,
            slot_offset: self.config.slot_offset,
            n,
            lexical,
            doc_filter,
            prune,
        })
    }

    fn run_browse_shard(
        &self,
        req: &crate::pb::BrowseShardRequest,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        for sort in &req.sort {
            crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let filter =
            crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
        if req.k == 0 {
            return Err(Status::invalid_argument("browse requires k > 0"));
        }
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let (_, visibility_columns_known) = filter_known_flags(
            guard.bm25.as_ref(),
            &[],
            req.visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        if !req.lexical_terms.is_empty() {
            guard.check_query_analysis("body", req.analysis_fingerprint)?;
        }
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
        let echo = BrowseEcho {
            guard: &guard,
            scope: &scope,
            visibility_columns_known,
            geo_columns_known,
            filter_columns_known,
        };
        let Some(store) = guard.bm25.as_ref() else {
            // A document-less shard admits nothing; its all-false known
            // flags feed the coordinator's typo rule like everywhere.
            return echo.respond(BrowsePage {
                sort_columns_known: vec![false; req.sort.len()],
                sort_column_types: req
                    .sort
                    .iter()
                    .map(|sort| configured_map_sort_type(&self.config, sort))
                    .collect(),
                ..Default::default()
            });
        };
        let walk = self.browse_walk(req, &guard, store, filter.as_ref(), &geo_regions)?;
        if !req.sort.is_empty() {
            return sorted_browse(&walk, echo);
        }
        echo.respond(BrowsePage {
            prune: walk.prune.stats,
            doc_ids: walk.id_ordered(),
            ..Default::default()
        })
    }

    /// Filter-only browse: admitted ids above the floor, ascending, at
    /// most k, or ordered by a column under `sort`. Match-all over the DOCUMENT space (bm25 postings), gated
    /// by the same resolved filter every scored route admits through;
    /// no scoring, no heap — the order is the id order.
    pub(in crate::node::rpc) async fn handle_browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            self.run_browse_shard(&request.into_inner())
        })
        .await
    }
}
