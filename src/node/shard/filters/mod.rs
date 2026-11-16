//! Filter expression compilation, string predicates, and sealed-segment pruning.

use crate::node::*;

mod strings;
mod tree;

impl Bm25Shard {
    /// Resolve validated geo filters against this shard's geo table; missing
    /// columns resolve to `None` (matching no documents on this shard).
    pub(in crate::node) fn resolve_geo_filters(
        &self,
        filters: &[crate::pb::GeoFilter],
        regions: &[crate::geo::GeoRegion],
    ) -> crate::geo::GeoFilters {
        crate::geo::GeoFilters {
            filters: filters
                .iter()
                .zip(regions)
                .map(|(f, &region)| crate::geo::GeoFilter {
                    column: self.geo_index(&f.column),
                    region,
                })
                .collect(),
        }
    }

    /// Which requested geo filters' columns this shard's table has,
    /// parallel to `filters`. Computed regardless of `k`, like the
    /// facet and stage known flags: a typo must refuse even on a query
    /// that returns nothing.
    pub(in crate::node) fn geo_columns_known(&self, filters: &[crate::pb::GeoFilter]) -> Vec<bool> {
        filters
            .iter()
            .map(|f| self.geo_index(&f.column).is_some())
            .collect()
    }
    /// Whether this shard can resolve each leaf of `expr`, positionally
    /// over [`crate::filter::walk_leaves`] order — the wire contract of
    /// `Bm25QueryResponse.filter_columns_known`. Computed regardless of
    /// `k`, like every other known flag: a typo must refuse even on a
    /// query that returns nothing. What "resolve" means per leaf kind
    /// is pinned on the FilterExpr proto messages.
    pub(in crate::node) fn filter_columns_known(&self, expr: &crate::pb::FilterExpr) -> Vec<bool> {
        let mut known = Vec::new();
        crate::filter::walk_leaves(expr, &mut |leaf| {
            use crate::filter::LeafRef;
            known.push(match leaf {
                LeafRef::Facet(p) => self.facet_index(&p.column).is_some(),
                LeafRef::Number(p) => {
                    self.integer_index(&p.column).is_some()
                        || self.unsigned_integer_index(&p.column).is_some()
                        || self.numeric_index(&p.column).is_some()
                }
                LeafRef::MapFacet(p) => self
                    .map_facet_index(&p.column)
                    .and_then(|ci| self.map_facet_key_ord(ci, &p.key))
                    .is_some(),
                LeafRef::MapNumber(p) | LeafRef::TypedMapNumber(p) => {
                    let typed = matches!(leaf, LeafRef::TypedMapNumber(_));
                    self.map_integer_index(&p.column)
                        .filter(|_| typed)
                        .and_then(|ci| self.map_integer_key_ord(ci, &p.key))
                        .is_some()
                        || self
                            .map_unsigned_integer_index(&p.column)
                            .filter(|_| typed)
                            .and_then(|ci| self.map_unsigned_integer_key_ord(ci, &p.key))
                            .is_some()
                        || self
                            .map_numeric_index(&p.column)
                            .and_then(|ci| self.map_numeric_key_ord(ci, &p.key))
                            .is_some()
                }
                LeafRef::MapHasKey(p) | LeafRef::TypedMapHasKey(p) => {
                    let typed = matches!(leaf, LeafRef::TypedMapHasKey(_));
                    self.map_facet_index(&p.column).is_some()
                        || self.map_numeric_index(&p.column).is_some()
                        || self
                            .map_integer_index(&p.column)
                            .filter(|_| typed)
                            .is_some()
                        || self
                            .map_unsigned_integer_index(&p.column)
                            .filter(|_| typed)
                            .is_some()
                }
                LeafRef::Has(p) => {
                    self.facet_index(&p.column).is_some()
                        || self.numeric_index(&p.column).is_some()
                        || self.integer_index(&p.column).is_some()
                        || self.unsigned_integer_index(&p.column).is_some()
                        || self.geo_index(&p.column).is_some()
                }
                LeafRef::Geo(g) => self.geo_index(&g.column).is_some(),
                LeafRef::StringRange(p) => self
                    .string_target_known(&p.column, (!p.key.is_empty()).then_some(p.key.as_str())),
                LeafRef::MapStringRange(p) => self.string_target_known(&p.column, Some(&p.key)),
                LeafRef::StringPrefix(p) => self
                    .string_target_known(&p.column, (!p.key.is_empty()).then_some(p.key.as_str())),
                LeafRef::MapStringPrefix(p) => self.string_target_known(&p.column, Some(&p.key)),
            });
        });
        known
    }

    /// The known rule of a string range / prefix: the facet column, or
    /// the map-facet column and its key (the FacetPredicate /
    /// MapFacetPredicate rules, since the predicate reads the same
    /// dictionaries).
    pub(in crate::node) fn string_target_known(&self, column: &str, key: Option<&str>) -> bool {
        match key {
            None => self.facet_index(column).is_some(),
            Some(key) => self
                .map_facet_index(column)
                .and_then(|ci| self.map_facet_key_ord(ci, key))
                .is_some(),
        }
    }
}

/// Sealed segments ruled out by a resolved predicate on one shard: skipped local
/// slot ranges, prune telemetry, and the admitted-parts mask (`None` when all pass).
#[derive(Debug, Default)]
pub(in crate::node) struct SegmentPrune {
    /// `(base, rows)` of every pruned sealed segment, ascending.
    pub(in crate::node) ranges: Vec<(u32, u32)>,
    pub(in crate::node) stats: crate::segment_prune::PruneStats,
    /// One flag per sealed part, `true` = walk it.
    pub(in crate::node) mask: Option<Arc<[bool]>>,
}

impl SegmentPrune {
    /// Local slots outside every pruned range, ascending, over `0..n`.
    pub(in crate::node) fn admitted_slots(&self, n: u32) -> impl Iterator<Item = u32> + '_ {
        self.admitted_slots_from(0, n)
    }

    /// Local slots outside every pruned range, ascending, over `start..n`.
    pub(in crate::node) fn admitted_slots_from(
        &self,
        start: u32,
        n: u32,
    ) -> impl Iterator<Item = u32> + '_ {
        let mut ranges = self.ranges.iter().copied().peekable();
        (start..n).filter(move |&slot| {
            while let Some(&(base, rows)) = ranges.peek() {
                if slot >= base.saturating_add(rows) {
                    ranges.next();
                    continue;
                }
                return slot < base;
            }
            true
        })
    }
}

/// Decide, from the segment summaries, which sealed segments of a
/// segmented shard hold no row that can satisfy `pred`. A shard in any
/// other layout, or a request without a predicate, prunes none; with
/// `enabled == false` none is pruned either, and only the total is
/// counted.
pub(in crate::node) fn prune_segments(
    store: &Bm25Shard,
    pred: Option<&crate::filter::ResolvedFilter>,
    enabled: bool,
) -> SegmentPrune {
    let Bm25Shard::Segmented(shard) = store else {
        return SegmentPrune::default();
    };
    let summaries = shard.part_summaries();
    let (verdicts, stats) = crate::segment_prune::verdicts_over(pred, &summaries, shard, enabled);
    let ranges: Vec<(u32, u32)> = shard
        .sealed_ranges()
        .into_iter()
        .zip(&verdicts)
        .filter(|(_, &pruned)| pruned)
        .map(|(range, _)| range)
        .collect();
    let mask = (stats.segments_skipped > 0).then(|| {
        verdicts
            .iter()
            .map(|&pruned| !pruned)
            .collect::<Arc<[bool]>>()
    });
    SegmentPrune {
        ranges,
        stats,
        mask,
    }
}

/// Resolved shard filter tuple: lexical heap-gate predicate, vector-kernel
/// slot allowlist, and segment-pruning counts.
pub(in crate::node) type ResolvedShardFilters = (
    Option<crate::filter::DocFilter<'static>>,
    Option<crate::boolean_bits::Bits>,
    crate::segment_prune::PruneStats,
);

/// Resolve a request's filters for one shard into its lexical predicate and
/// vector slot allowlist. With `prune` set on a segmented shard, sealed
/// segments ruled out by their summaries are skipped before per-row evaluation;
/// with `domain` set, only domain slots are evaluated.
#[allow(clippy::too_many_arguments)]
pub(in crate::node) fn resolve_shard_filters(
    bm25: Option<&Bm25Shard>,
    deleted: Option<Arc<Vec<u64>>>,
    n: usize,
    geo_filters: &[crate::pb::GeoFilter],
    geo_regions: &[crate::geo::GeoRegion],
    filter: Option<&crate::pb::FilterExpr>,
    prune: bool,
    domain: Option<&crate::boolean_bits::Bits>,
) -> Result<ResolvedShardFilters, Status> {
    let total = |bm25: Option<&Bm25Shard>| crate::segment_prune::PruneStats {
        segments_total: match bm25 {
            Some(Bm25Shard::Segmented(shard)) => shard.sealed_parts() as u32,
            _ => 0,
        },
        segments_skipped: 0,
    };
    if geo_filters.is_empty() && filter.is_none() && deleted.is_none() {
        return Ok((None, None, total(bm25)));
    }
    use crate::boolean_bits::Bits;
    let live = Bits::live(deleted.as_deref().map(Vec::as_slice), n);
    let Some(store) = bm25 else {
        if geo_filters.is_empty() && filter.is_none() {
            return Ok((None, Some(live), total(bm25)));
        }
        return Ok((None, Some(Bits::empty(n)), total(bm25)));
    };
    if store.as_index().is_none() {
        return Err(Status::failed_precondition(
            "bm25 bulk build in progress; Flush before filtering the vector leg",
        ));
    }
    let doc_filter = crate::filter::DocFilter {
        deleted,
        geo: store.resolve_geo_filters(geo_filters, geo_regions),
        pred: filter.map(|f| store.resolve_filter(f)).transpose()?,
        phrase: Vec::new(),
    };
    let pruned = prune_segments(store, doc_filter.pred.as_ref(), prune);
    // The rows still in question before any column is read: live, outside
    // every pruned segment, and inside the caller's domain.
    let mut open = live;
    for &(base, rows) in &pruned.ranges {
        open.clear_range(base as usize, base.saturating_add(rows) as usize);
    }
    if let Some(domain) = domain {
        open.and_with(domain);
    }
    let cols = ShardNumericRead(store);
    let mut allow = match &doc_filter.pred {
        Some(pred) => crate::filter_bits::members(pred, true, &open, &cols, store),
        None => open,
    };
    if !doc_filter.geo.filters.is_empty() {
        allow.retain(|slot| doc_filter.geo.passes(slot as u32, &cols));
    }
    Ok((Some(doc_filter), Some(allow), pruned.stats))
}

/// The two known-column handshakes for one request, in the shape every
/// response carries them: which requested geo columns this shard's
/// table has, and which leaves of the filter tree it can resolve
/// ([`crate::filter::walk_leaves`] order). Computed regardless of `k`
/// and regardless of whether the shard scores, so a typo refuses even
/// on a query that would legitimately return nothing.
pub(in crate::node) fn filter_known_flags(
    bm25: Option<&Bm25Shard>,
    geo_filters: &[crate::pb::GeoFilter],
    filter: Option<&crate::pb::FilterExpr>,
) -> (Vec<bool>, Vec<bool>) {
    let geo = match bm25 {
        Some(store) => store.geo_columns_known(geo_filters),
        None => vec![false; geo_filters.len()],
    };
    let tree = match (bm25, filter) {
        (Some(store), Some(f)) => store.filter_columns_known(f),
        (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
        (_, None) => Vec::new(),
    };
    (geo, tree)
}
