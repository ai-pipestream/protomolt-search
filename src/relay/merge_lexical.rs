//! Exact merges of lexical answers: term statistics, BM25 pages, boolean results.

use tonic::Status;

use crate::pb::{Bm25QueryRequest, Bm25QueryResponse, TermStatsRequest, TermStatsResponse};

use super::merge_util::{by_score_then_id, join_flags, merge_known, or_flags, or_into};

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
    let mut acc = empty_term_stats(request);
    for (shard, child) in children.iter().enumerate() {
        scope.validate_response(child)?;
        crate::visibility::validate_stats_mode(request.version_only, child)?;
        or_into(
            &mut visibility_columns_known,
            &child.visibility_columns_known,
        );
        add_term_stats_totals(&mut acc, shard, child)?;
        add_body_frequencies(request, &mut acc.doc_frequencies, shard, child)?;
        if child.field_stats.len() != acc.field_stats.len() {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} field shares for {} fields",
                child.field_stats.len(),
                acc.field_stats.len()
            )));
        }
        for (fi, (field, share)) in acc
            .field_stats
            .iter_mut()
            .zip(&child.field_stats)
            .enumerate()
        {
            add_field_share(&request.fields[fi], field, share, shard)?;
        }
    }
    acc.visibility_fingerprint = scope.fingerprint().to_vec();
    acc.visibility_columns_known = visibility_columns_known;
    Ok(acc)
}

/// The zero statistics a merge starts from, shaped by the request.
fn empty_term_stats(request: &TermStatsRequest) -> TermStatsResponse {
    TermStatsResponse {
        version_only: request.version_only,
        doc_count: 0,
        total_doc_length: 0,
        doc_frequencies: vec![0; request.terms.len()],
        field_stats: request
            .fields
            .iter()
            .map(|ft| crate::pb::FieldStats {
                total_doc_length: 0,
                doc_frequencies: vec![0; ft.terms.len()],
                known: false,
                positions: false,
                sentences: false,
            })
            .collect(),
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    }
}

fn add_term_stats_totals(
    acc: &mut TermStatsResponse,
    shard: usize,
    child: &TermStatsResponse,
) -> Result<(), Status> {
    acc.doc_count = acc.doc_count.checked_add(child.doc_count).ok_or_else(|| {
        Status::failed_precondition(format!(
            "relay: document count sums past u64 at child {shard}"
        ))
    })?;
    acc.total_doc_length = acc
        .total_doc_length
        .checked_add(child.total_doc_length)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: total document length sums past u64 at child {shard}"
            ))
        })?;
    Ok(())
}

fn add_body_frequencies(
    request: &TermStatsRequest,
    doc_frequencies: &mut [u32],
    shard: usize,
    child: &TermStatsResponse,
) -> Result<(), Status> {
    let n_terms = request.terms.len();
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
    Ok(())
}

/// One child's share of one field: capabilities must match child 0's,
/// lengths and frequencies add with a check.
fn add_field_share(
    terms: &crate::pb::FieldTerms,
    acc: &mut crate::pb::FieldStats,
    share: &crate::pb::FieldStats,
    shard: usize,
) -> Result<(), Status> {
    let field = &terms.field;
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
                terms.terms[ti]
            ))
        })?;
    }
    Ok(())
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
    let mut merged = crate::pb::BooleanShardResponse::default();
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
        merged.candidates.extend(share.candidates.iter().cloned());
        merged.matched = merged.matched.checked_add(share.matched).ok_or_else(|| {
            Status::internal("relay: Boolean match count overflows u64 across children")
        })?;
        merged.segments_total = merged.segments_total.saturating_add(share.segments_total);
        merged.segments_skipped = merged
            .segments_skipped
            .saturating_add(share.segments_skipped);
        join_boolean_known(shard, share, &mut filters_known, &mut stages_known)?;
    }
    merged
        .candidates
        .sort_by(|a, b| by_score_then_id((a.score, a.doc_id), (b.score, b.doc_id)));
    merged.candidates.truncate(req.depth as usize);
    merged.filters_known = filters_known
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect();
    merged.stages_known = stages_known
        .into_iter()
        .map(Option::unwrap_or_default)
        .collect();
    Ok(merged)
}

fn join_boolean_known(
    shard: usize,
    share: &crate::pb::BooleanShardResponse,
    filters_known: &mut [Option<crate::pb::BooleanFilterKnown>],
    stages_known: &mut [Option<crate::pb::BooleanStagesKnown>],
) -> Result<(), Status> {
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
    Ok(())
}

/// The known flags and counters a BM25 merge carries beside its hits.
#[derive(Default)]
struct Bm25Known {
    stage: Option<Vec<bool>>,
    geo: Option<Vec<bool>>,
    filter: Option<Vec<bool>>,
    projection: Option<Vec<bool>>,
    segments_total: u32,
    segments_skipped: u32,
}

impl Bm25Known {
    fn add(&mut self, shard: usize, share: &Bm25QueryResponse) -> Result<(), Status> {
        merge_known(
            "stage-column",
            shard,
            &mut self.stage,
            &share.stage_columns_known,
        )?;
        merge_known("geo-column", shard, &mut self.geo, &share.geo_columns_known)?;
        merge_known(
            "filter-leaf",
            shard,
            &mut self.filter,
            &share.filter_columns_known,
        )?;
        merge_known(
            "projection-leaf",
            shard,
            &mut self.projection,
            &share.projection_leaves_known,
        )?;
        self.segments_total = self
            .segments_total
            .checked_add(share.segments_total)
            .ok_or_else(|| Status::internal("relay: segments_total overflows u32"))?;
        self.segments_skipped = self
            .segments_skipped
            .checked_add(share.segments_skipped)
            .ok_or_else(|| Status::internal("relay: segments_skipped overflows u32"))?;
        Ok(())
    }
}

/// The children's terminal responses merged into the one the parent
/// reads as a shard's: every child's local top-k concatenated (the
/// parent's global merge picks from the union, and nothing this level
/// could drop is provably outside the parent's top-k), facet counts
/// summed by value, range buckets summed by position, column-known
/// flags ORed, segment counts added with a check. A facet no child knows
/// stays `known = false` for the root's typo rule; refusing it here
/// would answer for shards this relay does not see.
pub fn merge_bm25_responses(
    req: &Bm25QueryRequest,
    shares: Vec<Bm25QueryResponse>,
) -> Result<Bm25QueryResponse, Status> {
    crate::node::validate_range_facet_fields(&req.range_facet_fields)?;
    let mut range_shares = Vec::new();
    let mut hits = Vec::new();
    let mut facet_shares = Vec::new();
    let mut known = Bm25Known::default();
    let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
    for (shard, share) in shares.into_iter().enumerate() {
        known.add(shard, &share)?;
        crate::values::merge_projection_types(
            &req.projections,
            &mut projection_types,
            &share.projection_types,
        )?;
        for hit in &share.hits {
            crate::values::validate_projection_row(&hit.projected, &share.projection_types)?;
        }
        facet_shares.push(share.facets);
        range_shares.push(share.range_facets);
        hits.extend(share.hits);
    }
    // The monolith's order: score, then id. The parent re-merges under
    // its own total order; this order only decides the k-th seed below.
    hits.sort_by(|a, b| by_score_then_id((a.score, a.doc_id), (b.score, b.doc_id)));
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
        stage_columns_known: known.stage.unwrap_or_default(),
        range_facets,
        geo_columns_known: known.geo.unwrap_or_default(),
        filter_columns_known: known.filter.unwrap_or_default(),
        stats: Vec::new(),
        distinct: Vec::new(),
        projection_leaves_known: known.projection.unwrap_or_default(),
        projection_types: projection_types.into_iter().map(|ty| ty as i32).collect(),
        segments_total: known.segments_total,
        segments_skipped: known.segments_skipped,
    })
}
