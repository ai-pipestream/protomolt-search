//! Filter expression compilation, string predicates, and sealed-segment pruning.

use crate::node::*;

impl Bm25Shard {
    /// Resolve validated geo filters against THIS shard's geo table
    /// (`docs/geo-columns.md`). A column the table lacks resolves to
    /// `None`, which fails every document — its documents genuinely
    /// hold no location, so "not inside the region" is the exact
    /// answer. The coordinator refuses a column NO shard knows.
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

    /// Resolve a VALIDATED filter tree against this shard's tables
    /// (`docs/cel-filters.md`): names to table indices, string values
    /// to dictionary ordinals, number bounds into the resolved
    /// family's exact domain. A name (or map key) this shard lacks
    /// resolves to the absent case for every document — exact, the
    /// same argument as [`Self::resolve_geo_filters`] — and the
    /// coordinator refuses a leaf NO shard resolves.
    pub(in crate::node) fn resolve_filter(
        &self,
        expr: &crate::pb::FilterExpr,
    ) -> Result<crate::filter::ResolvedFilter, Status> {
        use crate::filter::{MapKeyRef, ResolvedFilter, ResolvedLeaf};
        use crate::pb::filter_expr::Expr;
        let sorted_ords = |mut ords: Vec<u32>| {
            ords.sort_unstable();
            ords.dedup();
            ords
        };
        Ok(
            match expr.expr.as_ref().expect("filter validated before resolve") {
                Expr::And(list) => ResolvedFilter::And(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Or(list) => ResolvedFilter::Or(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Not(child) => ResolvedFilter::Not(Box::new(self.resolve_filter(child)?)),
                Expr::StringRange(p) | Expr::MapStringRange(p) => ResolvedFilter::Leaf(
                    self.resolve_string_predicate(
                        &p.column,
                        (matches!(expr.expr, Some(Expr::MapStringRange(_))) || !p.key.is_empty())
                            .then_some(p.key.as_str()),
                        p.min.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                        p.max.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                        None,
                    )?,
                ),
                Expr::StringPrefix(p) | Expr::MapStringPrefix(p) => ResolvedFilter::Leaf(
                    self.resolve_string_predicate(
                        &p.column,
                        (matches!(expr.expr, Some(Expr::MapStringPrefix(_))) || !p.key.is_empty())
                            .then_some(p.key.as_str()),
                        None,
                        None,
                        Some(p.prefix.as_str()),
                    )?,
                ),
                Expr::Facet(p) => {
                    let column = self.facet_index(&p.column);
                    let ords = match column {
                        None => Vec::new(),
                        Some(fi) => sorted_ords(
                            p.values
                                .iter()
                                .filter_map(|v| self.facet_value_ord_of(fi, v))
                                .collect(),
                        ),
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::Facet { column, ords })
                }
                Expr::Number(p) => {
                    let lo = p.min.as_ref().and_then(crate::filter::edge_of);
                    let hi = p.max.as_ref().and_then(crate::filter::edge_of);
                    // i64 first, then f64 — the RangeFacetField resolution
                    // order, so the two number surfaces can never disagree
                    // about which table a name means.
                    let leaf = if let Some(ii) = self.integer_index(&p.column) {
                        let (lo, hi) = crate::filter::int_range(&lo, &hi);
                        ResolvedLeaf::IntRange { column: ii, lo, hi }
                    } else if let Some(ii) = self.unsigned_integer_index(&p.column) {
                        let (lo, hi) = crate::filter::uint_range(&lo, &hi);
                        ResolvedLeaf::UintRange { column: ii, lo, hi }
                    } else if let Some(ni) = self.numeric_index(&p.column) {
                        ResolvedLeaf::F64Range { column: ni, lo, hi }
                    } else {
                        ResolvedLeaf::NumberUnknown
                    };
                    ResolvedFilter::Leaf(leaf)
                }
                Expr::MapFacet(p) => {
                    let target = self
                        .map_facet_index(&p.column)
                        .and_then(|ci| self.map_facet_key_ord(ci, &p.key).map(|k| (ci, k)));
                    let ords = match target {
                        None => Vec::new(),
                        Some((ci, _)) => sorted_ords(
                            p.values
                                .iter()
                                .filter_map(|v| self.map_facet_value_ord_of(ci, v))
                                .collect(),
                        ),
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::MapFacet { target, ords })
                }
                Expr::MapNumber(p) | Expr::TypedMapNumber(p) => {
                    let typed = matches!(expr.expr, Some(Expr::TypedMapNumber(_)));
                    let lo = p.min.as_ref().and_then(crate::filter::edge_of);
                    let hi = p.max.as_ref().and_then(crate::filter::edge_of);
                    let leaf = if let Some(ci) = self.map_integer_index(&p.column).filter(|_| typed)
                    {
                        let target = self.map_integer_key_ord(ci, &p.key).map(|key| (ci, key));
                        let (lo, hi) = crate::filter::int_range(&lo, &hi);
                        ResolvedLeaf::MapIntRange { target, lo, hi }
                    } else if let Some(ci) =
                        self.map_unsigned_integer_index(&p.column).filter(|_| typed)
                    {
                        let target = self
                            .map_unsigned_integer_key_ord(ci, &p.key)
                            .map(|key| (ci, key));
                        let (lo, hi) = crate::filter::uint_range(&lo, &hi);
                        ResolvedLeaf::MapUintRange { target, lo, hi }
                    } else {
                        let target = self.map_numeric_index(&p.column).and_then(|ci| {
                            self.map_numeric_key_ord(ci, &p.key).map(|key| (ci, key))
                        });
                        ResolvedLeaf::MapNumber { target, lo, hi }
                    };
                    ResolvedFilter::Leaf(leaf)
                }
                Expr::MapHasKey(p) | Expr::TypedMapHasKey(p) => {
                    let typed = matches!(expr.expr, Some(Expr::TypedMapHasKey(_)));
                    // Map-facet first, then map-numeric: the one order,
                    // shared with `filter_columns_known` below.
                    let target = if let Some(ci) = self.map_facet_index(&p.column) {
                        MapKeyRef::Facet {
                            column: ci,
                            key_ord: self.map_facet_key_ord(ci, &p.key),
                        }
                    } else if let Some(ci) = self.map_numeric_index(&p.column) {
                        MapKeyRef::Numeric {
                            column: ci,
                            key_ord: self.map_numeric_key_ord(ci, &p.key),
                        }
                    } else if let Some(ci) = self.map_integer_index(&p.column).filter(|_| typed) {
                        MapKeyRef::Integer {
                            column: ci,
                            key_ord: self.map_integer_key_ord(ci, &p.key),
                        }
                    } else if let Some(ci) =
                        self.map_unsigned_integer_index(&p.column).filter(|_| typed)
                    {
                        MapKeyRef::UnsignedInteger {
                            column: ci,
                            key_ord: self.map_unsigned_integer_key_ord(ci, &p.key),
                        }
                    } else {
                        MapKeyRef::Unknown
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::MapHasKey(target))
                }
                Expr::Has(p) => ResolvedFilter::Leaf(ResolvedLeaf::Has {
                    facet: self.facet_index(&p.column),
                    numeric: self.numeric_index(&p.column),
                    integer: self.integer_index(&p.column),
                    unsigned_integer: self.unsigned_integer_index(&p.column),
                    geo: self.geo_index(&p.column),
                }),
                Expr::Geo(g) => ResolvedFilter::Leaf(ResolvedLeaf::Geo {
                    column: self.geo_index(&g.column),
                    region: validate_geo_filter(g).expect("filter validated before resolve"),
                }),
            },
        )
    }

    /// Resolve a string range (`min`/`max`, each `(value, exclusive)`)
    /// or a `prefix` over a facet column (`key` absent) or a map-facet
    /// value (`docs/prefix-terms.md`), to an ORDINAL RANGE when the
    /// column's dictionary is in byte order — every file this writer
    /// produces, checked at open — and to plain ordinal membership on
    /// the heap builder, whose first-seen dictionary is in heap and
    /// scanned once per request. A disk-resident dictionary in the
    /// older first-seen order refuses by name: an ordinal range over
    /// it would be a lie, and a per-document string walk is the thing
    /// this predicate exists not to do.
    pub(in crate::node) fn resolve_string_predicate(
        &self,
        column: &str,
        key: Option<&str>,
        min: Option<(&str, bool)>,
        max: Option<(&str, bool)>,
        prefix: Option<&str>,
    ) -> Result<crate::filter::ResolvedLeaf, Status> {
        use crate::filter::ResolvedLeaf;
        let admits = |value: &str| -> bool {
            let v = value.as_bytes();
            let above = min.is_none_or(|(b, exclusive)| {
                if exclusive {
                    v > b.as_bytes()
                } else {
                    v >= b.as_bytes()
                }
            });
            let below = max.is_none_or(|(b, exclusive)| {
                if exclusive {
                    v < b.as_bytes()
                } else {
                    v <= b.as_bytes()
                }
            });
            above && below && prefix.is_none_or(|p| v.starts_with(p.as_bytes()))
        };
        // The contiguous ordinal range of a byte-sorted dictionary.
        let ord_range = |dict: &[String]| -> (u32, u32) {
            let lo = dict.partition_point(|v| {
                let v = v.as_bytes();
                let under_min = min.is_some_and(|(b, exclusive)| {
                    if exclusive {
                        v <= b.as_bytes()
                    } else {
                        v < b.as_bytes()
                    }
                });
                under_min || prefix.is_some_and(|p| v < p.as_bytes())
            });
            let hi = dict.partition_point(|v| {
                let v = v.as_bytes();
                let in_prefix =
                    prefix.is_none_or(|p| v < p.as_bytes() || v.starts_with(p.as_bytes()));
                let under_max = max.is_none_or(|(b, exclusive)| {
                    if exclusive {
                        v < b.as_bytes()
                    } else {
                        v <= b.as_bytes()
                    }
                });
                in_prefix && under_max
            });
            (lo as u32, hi.max(lo) as u32)
        };
        let unordered = |what: String| {
            Status::failed_precondition(format!(
                "{what} was written with a first-seen (unordered) dictionary; string ordering \
                 and prefixes need a dictionary in byte order, which this version writes at \
                 flush — rebuild or reshard the generation"
            ))
        };
        if key.is_none() {
            let Some(fi) = self.facet_index(column) else {
                return Ok(ResolvedLeaf::FacetOrdRange {
                    column: None,
                    lo: 0,
                    hi: 0,
                });
            };
            return match self {
                Bm25Shard::Building(s) => {
                    if s.facet_dictionary_sorted(fi) {
                        let (lo, hi) = ord_range(s.facet_dictionary(fi));
                        Ok(ResolvedLeaf::FacetOrdRange {
                            column: Some(fi),
                            lo,
                            hi,
                        })
                    } else {
                        let ords = s
                            .facet_dictionary(fi)
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| admits(v))
                            .map(|(ord, _)| ord as u32)
                            .collect();
                        Ok(ResolvedLeaf::Facet {
                            column: Some(fi),
                            ords,
                        })
                    }
                }
                Bm25Shard::Segmented(g) => {
                    if g.facet_dictionary_sorted(fi) {
                        let (lo, hi) = ord_range(g.facet_dictionary(fi));
                        Ok(ResolvedLeaf::FacetOrdRange {
                            column: Some(fi),
                            lo,
                            hi,
                        })
                    } else {
                        let ords = g
                            .facet_dictionary(fi)
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| admits(v))
                            .map(|(ord, _)| ord as u32)
                            .collect();
                        Ok(ResolvedLeaf::Facet {
                            column: Some(fi),
                            ords,
                        })
                    }
                }
                Bm25Shard::Resident(r) => {
                    if !r.facet_dictionary_sorted(fi) {
                        return Err(unordered(format!("facet column {column:?}")));
                    }
                    let (lo, hi) = ord_range(r.facet_dictionary(fi));
                    Ok(ResolvedLeaf::FacetOrdRange {
                        column: Some(fi),
                        lo,
                        hi,
                    })
                }
                Bm25Shard::Spilling(_) => Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                )),
            };
        }
        let key = key.expect("plain column returned above");
        let Some((ci, key_ord)) = self
            .map_facet_index(column)
            .and_then(|ci| self.map_facet_key_ord(ci, key).map(|k| (ci, k)))
        else {
            return Ok(ResolvedLeaf::MapFacetOrdRange {
                target: None,
                lo: 0,
                hi: 0,
            });
        };
        match self {
            Bm25Shard::Building(s) => {
                if s.map_facet_values_sorted(ci) {
                    let (lo, hi) = ord_range(s.map_facet_values(ci));
                    Ok(ResolvedLeaf::MapFacetOrdRange {
                        target: Some((ci, key_ord)),
                        lo,
                        hi,
                    })
                } else {
                    let ords = s
                        .map_facet_values(ci)
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| admits(v))
                        .map(|(ord, _)| ord as u32)
                        .collect();
                    Ok(ResolvedLeaf::MapFacet {
                        target: Some((ci, key_ord)),
                        ords,
                    })
                }
            }
            Bm25Shard::Segmented(g) => {
                if g.map_facet_values_sorted(ci) {
                    let (lo, hi) = ord_range(g.map_facet_values(ci));
                    Ok(ResolvedLeaf::MapFacetOrdRange {
                        target: Some((ci, key_ord)),
                        lo,
                        hi,
                    })
                } else {
                    let ords = g
                        .map_facet_values(ci)
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| admits(v))
                        .map(|(ord, _)| ord as u32)
                        .collect();
                    Ok(ResolvedLeaf::MapFacet {
                        target: Some((ci, key_ord)),
                        ords,
                    })
                }
            }
            Bm25Shard::Resident(r) => {
                if !r.map_facet_values_sorted(ci) {
                    return Err(unordered(format!("map-facet column {column:?} (values)")));
                }
                let (lo, hi) = ord_range(r.map_facet_values(ci));
                Ok(ResolvedLeaf::MapFacetOrdRange {
                    target: Some((ci, key_ord)),
                    lo,
                    hi,
                })
            }
            Bm25Shard::Spilling(_) => Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            )),
        }
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

/// The sealed segments a resolved predicate rules out on one shard
/// (`docs/segment-pruning.md`): their local slot ranges in catalog
/// order, the counts, and the admitted-parts mask the postings walks
/// take (`None` when no part was ruled out, so the unmasked reader
/// serves and the path is the one it always was).
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

/// Resolve a request's filters for one shard into the predicate the
/// scorers gate with and the slot allowlist the vector kernel scans
/// under. With `prune` set and a segmented shard, sealed segments the
/// predicate cannot match are ruled out from their summaries first
/// (`docs/segment-pruning.md`): their slots are `false` without a
/// per-row evaluation, and the counts come back alongside. With
/// `domain` set, only its slots are evaluated at all — the caller
/// guarantees membership evaluation never consults the verdict off the domain.
/// What [`resolve_shard_filters`] hands back: the predicate for the
/// scorers' heap gate, the slot allowlist for the vector kernel, and
/// the segment counts.
pub(in crate::node) type ResolvedShardFilters = (
    Option<crate::filter::DocFilter<'static>>,
    Option<crate::boolean_bits::Bits>,
    crate::segment_prune::PruneStats,
);

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
