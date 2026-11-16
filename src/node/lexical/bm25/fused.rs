//! Fused multi-field BM25 execution against one shard generation.

use super::{FusedFields, FusedPhrase};
use crate::node::*;

impl NodeServiceImpl {
    /// Score fused fields against one shard generation and its resolved filters.
    pub(in crate::node) fn bm25_query_fused(
        &self,
        req: &Bm25QueryRequest,
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let regions = validate_fused_request(req)?;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        for leg in &req.fields {
            guard.check_query_analysis(&leg.field, leg.analysis_fingerprint)?;
        }
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
        let mut response = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 || fused_has_facets(req) => {
                let execution = FusedExecution {
                    service: self,
                    req,
                    guard: &guard,
                    store,
                };
                execution.run(&regions, phrase, live, candidate)?
            }
            _ => unknown_fused_response(req),
        };
        response.geo_columns_known = geo_columns_known;
        response.filter_columns_known = filter_columns_known;
        response.kth_best = if response.hits.len() == req.k as usize {
            response
                .hits
                .last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(response)
    }
}

struct FusedExecution<'a> {
    service: &'a NodeServiceImpl,
    req: &'a Bm25QueryRequest,
    guard: &'a ShardState,
    store: &'a Bm25Shard,
}

impl FusedExecution<'_> {
    fn run(
        &self,
        regions: &[crate::geo::GeoRegion],
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = self.req;
        let store = self.store;
        let mut fields = FusedFields::new(req, store)?;
        let numeric_read = ShardNumericRead(store);
        let pred = req
            .filter
            .as_ref()
            .map(|f| store.resolve_filter(f))
            .transpose()?;
        let prune = prune_segments(store, pred.as_ref(), self.service.knobs.segment_pruning());
        fields.apply_mask(store, prune.mask.as_ref());
        let phrase_gates = fields.phrase_gates()?;
        let doc_filter = crate::filter::DocFilter {
            deleted: self.guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, regions),
            pred,
            phrase: phrase_gates,
        };
        let filter = if req.geo_filters.is_empty()
            && doc_filter.pred.is_none()
            && doc_filter.deleted.is_none()
            && doc_filter.phrase.is_empty()
        {
            None
        } else {
            Some((
                &doc_filter,
                &numeric_read as &dyn crate::scorefn::NumericRead,
            ))
        };
        let mut response = Bm25QueryResponse {
            segments_total: prune.stats.segments_total,
            segments_skipped: prune.stats.segments_skipped,
            ..Default::default()
        };
        fields.count_facets(store, filter, &mut response)?;
        let queries = fields.queries()?;
        let phrase = fields.phrase_view(phrase)?;
        let request = bm25::TopK::new(req.k as usize)
            .floor(super::min_score_floor(req.min_score))
            .filter(filter)
            .live(live)
            .candidate(candidate)
            .block_max(self.service.config.block_max);
        let docs = score_fused(&queries, request, phrase.as_ref());
        response.hits = fields.render_hits(
            self.store,
            &queries,
            docs,
            self.service.config.slot_offset,
            phrase.as_ref(),
        )?;
        Ok(response)
    }
}

fn score_fused(
    queries: &[bm25::FieldQuery],
    mut request: bm25::TopK<'_>,
    phrase: Option<&FusedPhrase>,
) -> Vec<bm25::FusedDoc> {
    let Some((field, weights)) = phrase else {
        return bm25::top_k_fused(queries, request);
    };
    let mut sink = request.candidate.take();
    let phrase = bm25::Phrase {
        field: *field,
        weights,
    };
    let docs = bm25::top_k_fused_exhaustive(queries, request, Some(phrase));
    if let Some(sink) = sink.as_mut() {
        for doc in &docs {
            sink(doc.doc_id, doc.score as f32);
        }
    }
    docs
}

fn validate_fused_request(req: &Bm25QueryRequest) -> Result<Vec<crate::geo::GeoRegion>, Status> {
    if !req.projections.is_empty() {
        return Err(Status::invalid_argument(
            "projection: projections are not certified on the fused route yet; \
             use the flat Bm25Search route",
        ));
    }
    for leg in &req.fields {
        if leg.terms.len() != leg.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(format!(
                "leg {:?}: terms and global_doc_frequencies must have the same length",
                leg.field
            )));
        }
        if leg.weight < 0.0 || leg.weight.is_nan() {
            return Err(Status::invalid_argument(format!(
                "leg {:?}: weight must be >= 0",
                leg.field
            )));
        }
    }
    validate_range_facet_fields(&req.range_facet_fields)?;
    let regions = validate_geo_filters(&req.geo_filters)?;
    if let Some(f) = req.filter.as_ref() {
        crate::filter::validate_filter(f)?;
    }
    Ok(regions)
}

pub(super) fn fused_has_facets(req: &Bm25QueryRequest) -> bool {
    !req.facet_fields.is_empty()
        || !req.map_facet_fields.is_empty()
        || !req.range_facet_fields.is_empty()
}

fn unknown_fused_response(req: &Bm25QueryRequest) -> Bm25QueryResponse {
    Bm25QueryResponse {
        facets: req
            .facet_fields
            .iter()
            .map(|name| (name.clone(), None))
            .chain(
                req.map_facet_fields
                    .iter()
                    .map(|m| (m.column.clone(), Some(m.key.clone()))),
            )
            .map(|(field, key)| crate::pb::FacetFieldCounts {
                field,
                known: false,
                counts: Vec::new(),
                map_key: key,
            })
            .collect(),
        range_facets: unknown_range_counts(&req.range_facet_fields),
        ..Default::default()
    }
}
