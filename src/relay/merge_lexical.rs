//! Exact merges of lexical answers: term statistics, BM25 pages, boolean results.

use tonic::Status;

use crate::coordinator::RequestFilters;
use crate::pb::{
    Bm25QueryRequest, Bm25QueryResponse, HealthResponse, TermStatsRequest, TermStatsResponse,
};

use super::health::{health_span, merge_health};

/// Sum the children's statistics shares into the parent's view of one
/// shard. Checked arithmetic throughout: a `u32` document frequency or
/// a `u64` total that would overflow refuses by name, because a wrong
/// idf is a wrong ranking and not a warning. Field capabilities must
/// agree across children: `known`, `positions`, and `sentences` are one
/// boolean each on the wire, and a mixed answer has no faithful spelling
/// in one (a phrase needs positions EVERYWHERE, a typo check needs the
/// field SOMEWHERE), so a relay refuses the mixture by name. The epoch is
/// left at 0 for the caller to replace with a token.
pub fn merge_term_stats(
    request: &TermStatsRequest,
    children: &[TermStatsResponse],
) -> Result<TermStatsResponse, Status> {
    crate::visibility::validate_stats_request(request)?;
    let scope = crate::visibility::VisibilityScope::new(request.visibility.as_ref())?;
    let mut visibility_columns_known = vec![false; scope.column_count()];
    if children.is_empty() {
        return Err(Status::failed_precondition(
            "relay: TermStats over no children",
        ));
    }
    let n_terms = request.terms.len();
    let mut doc_count: u64 = 0;
    let mut total_doc_length: u64 = 0;
    let mut doc_frequencies: Vec<u32> = vec![0; n_terms];
    let mut field_stats: Vec<crate::pb::FieldStats> = request
        .fields
        .iter()
        .map(|ft| crate::pb::FieldStats {
            total_doc_length: 0,
            doc_frequencies: vec![0; ft.terms.len()],
            known: false,
            positions: false,
            sentences: false,
        })
        .collect();
    for (shard, child) in children.iter().enumerate() {
        scope.validate_response(child)?;
        crate::visibility::validate_stats_mode(request.version_only, child)?;
        for (known, child_known) in visibility_columns_known
            .iter_mut()
            .zip(&child.visibility_columns_known)
        {
            *known |= child_known;
        }
        doc_count = doc_count.checked_add(child.doc_count).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: document count sums past u64 at child {shard}"
            ))
        })?;
        total_doc_length = total_doc_length
            .checked_add(child.total_doc_length)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: total document length sums past u64 at child {shard}"
                ))
            })?;
        if child.doc_frequencies.len() != n_terms {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} body document frequencies for {n_terms} terms",
                child.doc_frequencies.len()
            )));
        }
        for (ti, (acc, df)) in doc_frequencies
            .iter_mut()
            .zip(&child.doc_frequencies)
            .enumerate()
        {
            *acc = acc.checked_add(*df).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: document frequency of body term {:?} sums past the u32 the \
                     statistics contract carries, at child {shard}; the collection is \
                     larger than the contract admits",
                    request.terms[ti]
                ))
            })?;
        }
        if child.field_stats.len() != field_stats.len() {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} field shares for {} fields",
                child.field_stats.len(),
                field_stats.len()
            )));
        }
        for (fi, (acc, share)) in field_stats.iter_mut().zip(&child.field_stats).enumerate() {
            let field = &request.fields[fi].field;
            if shard == 0 {
                acc.known = share.known;
                acc.positions = share.positions;
                acc.sentences = share.sentences;
            } else {
                for (name, mine, theirs) in [
                    ("known", acc.known, share.known),
                    ("positions", acc.positions, share.positions),
                    ("sentences", acc.sentences, share.sentences),
                ] {
                    if mine != theirs {
                        return Err(Status::failed_precondition(format!(
                            "relay: field {field:?} is {name}={mine} on child 0 and \
                             {name}={theirs} on child {shard}; a relay requires homogeneous \
                             field capabilities across its children"
                        )));
                    }
                }
            }
            acc.total_doc_length = acc
                .total_doc_length
                .checked_add(share.total_doc_length)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: total length of field {field:?} sums past u64 at child {shard}"
                    ))
                })?;
            if share.doc_frequencies.len() != acc.doc_frequencies.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} document frequencies for {} terms of \
                     field {field:?}",
                    share.doc_frequencies.len(),
                    acc.doc_frequencies.len()
                )));
            }
            for (ti, (a, df)) in acc
                .doc_frequencies
                .iter_mut()
                .zip(&share.doc_frequencies)
                .enumerate()
            {
                *a = a.checked_add(*df).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: document frequency of term {:?} in field {field:?} sums past \
                         the u32 the statistics contract carries, at child {shard}",
                        request.fields[fi].terms[ti]
                    ))
                })?;
            }
        }
    }
    Ok(TermStatsResponse {
        version_only: request.version_only,
        doc_count,
        total_doc_length,
        doc_frequencies,
        field_stats,
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
    })
}

/// The two request shapes this level does not merge: column statistics
/// fold floating-point partials in an order the root pins, and an exact
/// cardinality is a union of values, not a sum. Both are refused by name
/// rather than answered with a different reduction.
pub(super) fn refuse_bm25_aggregates(
    route: &str,
    stats_fields: &[String],
    cardinality_fields: &[String],
) -> Result<(), Status> {
    if !stats_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with stats_fields {stats_fields:?} is not served through a relay; \
             a column statistic folds in the root's shard order and this level would change \
             it"
        )));
    }
    if !cardinality_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with cardinality_fields {cardinality_fields:?} is not served \
             through a relay; an exact cardinality is a union of values, not a sum of counts"
        )));
    }
    Ok(())
}

/// OR one child's per-column flags into the accumulator, which every
/// child must size the same way.
pub(super) fn merge_known(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<bool>>,
    share: &[bool],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(flags) => {
            if flags.len() != share.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} {what} flags while an earlier child \
                     answered {}",
                    share.len(),
                    flags.len()
                )));
            }
            for (a, b) in flags.iter_mut().zip(share) {
                *a |= *b;
            }
        }
    }
    Ok(())
}

/// The children's terminal responses merged into the one the parent
/// reads as a shard's: every child's local top-k concatenated (the
/// parent's global merge picks from the union, and nothing this level
/// could drop is provably outside the parent's top-k), facet counts
/// summed by value, range buckets summed by position, column-known
/// flags ORed, segment counts added with a check. A facet no child knows
/// stays `known = false` for the root's typo rule; refusing it here
/// would answer for shards this relay does not see.
/// The ranked union of the children's Boolean answers cut to `depth`,
/// score descending then doc id ascending (the root's own order), with
/// the match counts and segment counts summed and each leaf's known
/// flags joined by OR. A child answering a flag list of another shape
/// than the request's leaves is a protocol break.
pub fn merge_boolean_responses(
    req: &crate::pb::BooleanShardRequest,
    shares: &[crate::pb::BooleanShardResponse],
) -> Result<crate::pb::BooleanShardResponse, Status> {
    let leaves = req.leaves.len();
    let mut candidates = Vec::new();
    let mut matched = 0u64;
    let mut segments_total = 0u32;
    let mut segments_skipped = 0u32;
    let mut filters_known: Vec<Option<crate::pb::BooleanFilterKnown>> = vec![None; leaves];
    let mut stages_known: Vec<Option<crate::pb::BooleanStagesKnown>> = vec![None; leaves];
    for (shard, share) in shares.iter().enumerate() {
        if share.filters_known.len() != leaves || share.stages_known.len() != leaves {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} filter and {} stage flag lists for {leaves} leaves",
                share.filters_known.len(),
                share.stages_known.len()
            )));
        }
        candidates.extend(share.candidates.iter().cloned());
        matched = matched.checked_add(share.matched).ok_or_else(|| {
            Status::internal("relay: Boolean match count overflows u64 across children")
        })?;
        segments_total = segments_total.saturating_add(share.segments_total);
        segments_skipped = segments_skipped.saturating_add(share.segments_skipped);
        for (index, known) in share.filters_known.iter().enumerate() {
            join_flags(&mut filters_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.geo_columns_known,
                    &k.geo_columns_known,
                    shard,
                    "geo",
                )?;
                or_flags(
                    &mut acc.filter_columns_known,
                    &k.filter_columns_known,
                    shard,
                    "filter-leaf",
                )
            })?;
        }
        for (index, known) in share.stages_known.iter().enumerate() {
            join_flags(&mut stages_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.stage_columns_known,
                    &k.stage_columns_known,
                    shard,
                    "stage",
                )
            })?;
        }
    }
    candidates.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    candidates.truncate(req.depth as usize);
    Ok(crate::pb::BooleanShardResponse {
        read_receipt: None,
        candidates,
        matched,
        segments_total,
        segments_skipped,
        stats_epoch: 0,
        filters_known: filters_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        stages_known: stages_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        aggregate: None,
    })
}

fn join_flags<T: Clone>(
    acc: &mut Option<T>,
    share: &T,
    join: impl FnOnce(&mut T, &T) -> Result<(), Status>,
) -> Result<(), Status> {
    match acc {
        None => {
            *acc = Some(share.clone());
            Ok(())
        }
        Some(acc) => join(acc, share),
    }
}

fn or_flags(acc: &mut [bool], share: &[bool], shard: usize, what: &str) -> Result<(), Status> {
    if acc.len() != share.len() {
        return Err(Status::internal(format!(
            "relay: child {shard} answered {} {what} flags while an earlier child answered {}",
            share.len(),
            acc.len()
        )));
    }
    for (a, k) in acc.iter_mut().zip(share) {
        *a |= *k;
    }
    Ok(())
}

pub fn merge_bm25_responses(
    req: &Bm25QueryRequest,
    shares: Vec<Bm25QueryResponse>,
) -> Result<Bm25QueryResponse, Status> {
    crate::node::validate_range_facet_fields(&req.range_facet_fields)?;
    let mut range_shares = Vec::new();
    let mut hits = Vec::new();
    let mut facet_shares = Vec::new();
    let mut stage_known = None;
    let mut geo_known = None;
    let mut filter_known = None;
    let mut projection_known = None;
    let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
    let mut segments_total: u32 = 0;
    let mut segments_skipped: u32 = 0;
    for (shard, share) in shares.into_iter().enumerate() {
        facet_shares.push(share.facets);
        range_shares.push(share.range_facets);
        merge_known(
            "stage-column",
            shard,
            &mut stage_known,
            &share.stage_columns_known,
        )?;
        merge_known(
            "geo-column",
            shard,
            &mut geo_known,
            &share.geo_columns_known,
        )?;
        merge_known(
            "filter-leaf",
            shard,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "projection-leaf",
            shard,
            &mut projection_known,
            &share.projection_leaves_known,
        )?;
        segments_total = segments_total
            .checked_add(share.segments_total)
            .ok_or_else(|| Status::internal("relay: segments_total overflows u32"))?;
        segments_skipped = segments_skipped
            .checked_add(share.segments_skipped)
            .ok_or_else(|| Status::internal("relay: segments_skipped overflows u32"))?;
        crate::values::merge_projection_types(
            &req.projections,
            &mut projection_types,
            &share.projection_types,
        )?;
        for hit in &share.hits {
            crate::values::validate_projection_row(&hit.projected, &share.projection_types)?;
        }
        hits.extend(share.hits);
    }
    // The monolith's order: score, then id. The parent re-merges under
    // its own total order; this order only decides the k-th seed below.
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    let k = req.k as usize;
    let kth_best = if k > 0 && hits.len() >= k {
        crate::bm25::floor_seed(hits[k - 1].score)
    } else {
        0.0
    };
    let facets = crate::coordinator::merge_facet_counts(
        &req.facet_fields,
        &req.map_facet_fields,
        &facet_shares,
        false,
    )?;
    let range_facets = crate::rangefacet::merge(&req.range_facet_fields, &range_shares, false)?;
    Ok(Bm25QueryResponse {
        hits,
        kth_best,
        facets,
        stage_columns_known: stage_known.unwrap_or_default(),
        range_facets,
        geo_columns_known: geo_known.unwrap_or_default(),
        filter_columns_known: filter_known.unwrap_or_default(),
        stats: Vec::new(),
        distinct: Vec::new(),
        projection_leaves_known: projection_known.unwrap_or_default(),
        projection_types: projection_types.into_iter().map(|ty| ty as i32).collect(),
        segments_total,
        segments_skipped,
    })
}

/// Which child holds a global id: the one whose slot range (from its
/// health report) contains it. Contiguity was checked at startup; a gap
/// that appeared since refuses by name here.
pub(super) fn child_of_id(ranges: &[(u64, u64)], id: u64) -> Option<usize> {
    ranges
        .iter()
        .position(|&(offset, span)| id >= offset && id - offset < span)
}

// --- The vector-side routes, the bitmap routes, and the dictionaries ---

/// The known-column accumulator of one fan-out at this level: geo flags
/// ORed, filter-leaf flags ORed over the request's tree with each
/// child's answer mapped through the leaves its placement code implied
/// away (`ShardMask::implied`: the child was sent a shorter tree and
/// answers in that order), and the leaves the mask itself resolved
/// marked from the start (`ShardMask::known`).
pub(super) struct KnownFlags {
    pub(super) geo: Vec<bool>,
    pub(super) tree: Vec<bool>,
}

impl KnownFlags {
    pub(super) fn new(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        let mut tree = vec![false; leaves];
        if let Some(mask) = mask {
            for &leaf in &mask.known {
                if let Some(flag) = tree.get_mut(leaf) {
                    *flag = true;
                }
            }
        }
        KnownFlags {
            geo: vec![false; filters.geo.len()],
            tree,
        }
    }

    pub(super) fn merge(
        &mut self,
        child: usize,
        mask: Option<&crate::placement::ShardMask>,
        geo: &[bool],
        tree: &[bool],
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} geo-column flags for {} geo filters",
                geo.len(),
                self.geo.len()
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        let dropped: &[usize] = mask
            .and_then(|m| m.implied.get(child))
            .map_or(&[], Vec::as_slice);
        let kept: Vec<usize> = (0..self.tree.len())
            .filter(|index| !dropped.contains(index))
            .collect();
        if tree.len() != kept.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} filter-leaf flags for the {} leaves it was \
                 sent (of {})",
                tree.len(),
                kept.len(),
                self.tree.len()
            )));
        }
        for (&index, k) in kept.iter().zip(tree) {
            self.tree[index] |= *k;
        }
        Ok(())
    }
}

/// The slot range of each child, `(offset, span)`, from the children's
/// health reports, with the contiguity rule applied: a gap or an overlap
/// refuses by name, because an id routed by these ranges, or a bitmap
/// laid out over them, would otherwise be wrong without a word.
pub(super) fn child_ranges(
    collection: &str,
    children: &[String],
    reports: &[HealthResponse],
) -> Result<Vec<(u64, u64)>, Status> {
    merge_health(collection, children, reports)?;
    Ok(reports
        .iter()
        .map(|r| (r.slot_offset, health_span(r)))
        .collect())
}

/// Sort ids into the child whose slot range holds each. An id in no
/// child's range is another shard's and is dropped, which is what a
/// node does with an id outside its own range (the boolean planner and
/// the exact rerank send every shard every candidate; the cascade routes
/// by shard, and a relay is one shard to it).
pub(super) fn route_ids(ranges: &[(u64, u64)], ids: &[u64]) -> Result<Vec<Vec<u64>>, Status> {
    let mut by_child: Vec<Vec<u64>> = vec![Vec::new(); ranges.len()];
    for &id in ids {
        if let Some(child) = child_of_id(ranges, id) {
            by_child[child].push(id);
        }
    }
    Ok(by_child)
}

/// Overflow-checked `u64` sum for a counter the children report.
pub(super) fn add_count(acc: &mut u64, share: u64, child: usize, what: &str) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// Overflow-checked `u32` sum for a counter the children report.
pub(super) fn add_count32(
    acc: &mut u32,
    share: u32,
    child: usize,
    what: &str,
) -> Result<(), Status> {
    *acc = acc.checked_add(share).ok_or_else(|| {
        Status::failed_precondition(format!("relay: child {child} overflows the summed {what}"))
    })?;
    Ok(())
}

/// The children's prefix expansions as one dictionary's: the union in
/// byte order and its exact size when every child stayed within the
/// cap; a child past the cap answers no terms and a count above it, and
/// the relay then answers the largest such count (a lower bound on the
/// subtree's, enough for the root's refusal) and no terms, as a node
/// past the cap does. `known` when any child knows the field.
pub fn merge_prefix_expansions(
    cap: u32,
    shares: &[crate::pb::ExpandTermPrefixResponse],
) -> crate::pb::ExpandTermPrefixResponse {
    let mut union = std::collections::BTreeSet::new();
    let mut known = false;
    let mut past_cap: u64 = 0;
    for share in shares {
        if !share.known {
            continue;
        }
        known = true;
        if share.count > u64::from(cap) {
            past_cap = past_cap.max(share.count);
            continue;
        }
        union.extend(share.terms.iter().cloned());
    }
    if past_cap > 0 {
        return crate::pb::ExpandTermPrefixResponse {
            terms: Vec::new(),
            count: past_cap,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
        };
    }
    let count = union.len() as u64;
    crate::pb::ExpandTermPrefixResponse {
        terms: if count > u64::from(cap) {
            Vec::new()
        } else {
            union.into_iter().collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    }
}

/// The children's dictionary scans as one: entries unioned in byte order
/// with each term's df summed (checked), the tombstone count summed, the
/// exact union size as the count within the bound; a child past the scan
/// bound makes the relay answer the largest such count and no entries,
/// as a node past the bound does.
pub fn merge_suggest_terms(
    max_scan: u64,
    shares: &[crate::pb::SuggestTermsResponse],
) -> Result<crate::pb::SuggestTermsResponse, Status> {
    let mut union: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut known = false;
    let mut tombstoned: u64 = 0;
    let mut past_bound: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        add_count(
            &mut tombstoned,
            share.tombstoned_rows,
            child,
            "tombstone count",
        )?;
        if !share.known {
            continue;
        }
        known = true;
        if share.count > max_scan {
            past_bound = past_bound.max(share.count);
            continue;
        }
        for entry in &share.entries {
            let df = union.entry(entry.term.clone()).or_insert(0);
            add_count(df, entry.df, child, "posting frequency")?;
        }
    }
    if past_bound > 0 {
        return Ok(crate::pb::SuggestTermsResponse {
            entries: Vec::new(),
            count: past_bound,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            tombstoned_rows: tombstoned,
        });
    }
    let count = union.len() as u64;
    Ok(crate::pb::SuggestTermsResponse {
        entries: if count > max_scan {
            Vec::new()
        } else {
            union
                .into_iter()
                .map(|(term, df)| crate::pb::SuggestTermEntry { term, df })
                .collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
        tombstoned_rows: tombstoned,
    })
}
