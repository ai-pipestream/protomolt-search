//! BM25 term/phrase queries, rescoring, highlighting, and dictionary scans.

use crate::node::*;

mod dictionary;
mod explain;
mod flat;
mod flat_hits;
mod fused;
mod fused_fields;
mod snippets;

use explain::explain_stages;
pub(in crate::node) use explain::explain_terms;
use fused_fields::{FusedFields, FusedPhrase};
pub(in crate::node) use snippets::{cut_snippets, highlight_plan};

/// Identify the score space, excluding delivery, filters, projections, and epoch claims.
pub(in crate::node) fn bm25_scoring_fingerprint(req: &Bm25QueryRequest) -> String {
    let mut canonical = req.clone();
    canonical.k = 0;
    canonical.min_score = 0.0;
    canonical.expected_stats_epoch = 0;
    canonical.expected_stats_incarnation.clear();
    canonical.facet_fields.clear();
    canonical.map_facet_fields.clear();
    canonical.range_facet_fields.clear();
    canonical.geo_filters.clear();
    canonical.filter = None;
    canonical.stats_fields.clear();
    canonical.cardinality_fields.clear();
    canonical.projections.clear();
    turbo_sha256::sha256(&prost::Message::encode_to_vec(&canonical)).to_hex()
}

/// Validate BM25 parameters required for sound bounds; zero selects the default.
pub(in crate::node) fn params_from(k1: f32, b: f32) -> Result<Bm25Params, Status> {
    if !k1.is_finite() || k1 < 0.0 {
        return Err(Status::invalid_argument(format!(
            "bm25 k1 must be finite and >= 0, got {k1}"
        )));
    }
    if !b.is_finite() || !(0.0..=1.0).contains(&b) {
        return Err(Status::invalid_argument(format!(
            "bm25 b must be finite and within [0, 1], got {b}"
        )));
    }
    Ok(Bm25Params {
        k1: if k1 == 0.0 {
            bm25::DEFAULT_K1
        } else {
            f64::from(k1)
        },
        b: if b == 0.0 {
            bm25::DEFAULT_B
        } else {
            f64::from(b)
        },
    })
}

impl NodeServiceImpl {
    pub(in crate::node) fn run_bm25_query(
        &self,
        req: Bm25QueryRequest,
    ) -> Result<Bm25QueryResponse, Status> {
        self.run_bm25_query_live(req, None, None)
    }

    pub(in crate::node) fn run_bm25_phrase_query(
        &self,
        request: crate::pb::Bm25PhraseQueryRequest,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = request
            .query
            .ok_or_else(|| Status::invalid_argument("phrase query is missing its BM25 query"))?;
        let phrase_leg = request.phrase_leg as usize;
        let Some(leg) = req.fields.get(phrase_leg) else {
            return Err(Status::invalid_argument(format!(
                "phrase_leg {} is outside {} BM25 fields",
                request.phrase_leg,
                req.fields.len()
            )));
        };
        if request.phrase_term_weights.len() != leg.terms.len() {
            return Err(Status::invalid_argument(
                "phrase_term_weights must be parallel to the selected phrase leg's terms",
            ));
        }
        if request
            .phrase_term_weights
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(Status::invalid_argument(
                "phrase term weights must be finite and non-negative",
            ));
        }
        let phrases = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this node has no phrase glossary configured")
        })?;
        if leg.field != phrases.phrase_field() {
            return Err(Status::invalid_argument(format!(
                "phrase leg names {:?}, but this node's phrase field is {:?}",
                leg.field,
                phrases.phrase_field()
            )));
        }
        if leg.analysis_fingerprint != phrases.fingerprint() {
            return Err(Status::failed_precondition(format!(
                "phrase query fingerprint {:016x} differs from configured vocabulary {:016x}",
                leg.analysis_fingerprint,
                phrases.fingerprint()
            )));
        }
        self.bm25_query_fused(
            &req,
            Some((phrase_leg, &request.phrase_term_weights)),
            None,
            None,
        )
    }

    /// Execute lexical scoring with optional live floors and compact candidates.
    pub(in crate::node) fn run_bm25_query_live(
        &self,
        req: Bm25QueryRequest,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        if !req.fields.is_empty() {
            if !req.score_stages.is_empty() {
                return Err(Status::invalid_argument(
                    "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                ));
            }
            return self.bm25_query_fused(&req, None, live, candidate);
        }
        FlatRun::new(self, &req)?.execute(live, candidate)
    }

    pub(in crate::node) fn run_bm25_rescore(
        &self,
        req: Bm25RescoreRequest,
    ) -> Result<Bm25RescoreResponse, Status> {
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
        let params = params_from(req.k1, req.b)?;
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let offset = self.config.slot_offset;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        guard.check_query_analysis("body", req.analysis_fingerprint)?;
        let (_, visibility_columns_known) = filter_known_flags(
            guard.bm25.as_ref(),
            &[],
            req.visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        let (hits, stage_columns_known) = match guard.bm25.as_ref() {
            Some(store) => rescore_store(&guard, store, &req, params, &stage_specs, offset)?,
            None => (Vec::new(), vec![false; stage_specs.len()]),
        };
        Ok(Bm25RescoreResponse {
            hits,
            stage_columns_known,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        })
    }
}

/// Score a rescore request's visible candidates on this shard, returning
/// the hits and which score-stage columns the shard knows.
fn rescore_store(
    guard: &ShardState,
    store: &Bm25Shard,
    req: &Bm25RescoreRequest,
    params: Bm25Params,
    stage_specs: &[ScoreStageSpec],
    offset: u64,
) -> Result<(Vec<Bm25Hit>, Vec<bool>), Status> {
    let stats = bm25::CorpusStats {
        doc_count: req.global_doc_count,
        total_doc_length: req.global_total_doc_length,
        dfs: req.global_doc_frequencies.clone(),
    };
    // Route global ids to this shard's local range.
    let local: Vec<u32> = guard
        .visible_candidate_slots(
            req.visibility.as_ref(),
            &req.candidate_ids,
            offset,
            store.next_doc_id() as usize,
        )?
        .into_iter()
        .map(|slot| slot as u32)
        .collect();
    let index = store.as_index();
    let chain = store.resolve_chain(stage_specs);
    let stage_columns_known = chain
        .stages
        .iter()
        .map(|stage| stage.column.is_some())
        .collect();
    let numeric_read = ShardNumericRead(store);
    let hits = bm25::score_candidates(index, &req.terms, &stats, params, &local)
        .into_iter()
        .map(|doc| Bm25Hit {
            identity: store.document_identity(doc.doc_id),
            explain: None,
            snippets: Vec::new(),
            projected: Vec::new(),
            doc_id: offset + u64::from(doc.doc_id),
            score: chain.eval(doc.score, doc.doc_id, &numeric_read) as f32,
            terms: doc
                .term_offsets
                .into_iter()
                .map(|(ti, offsets)| TermOccurrences {
                    term: req.terms[ti].clone(),
                    offsets: offsets
                        .into_iter()
                        .map(|(start, end)| OffsetSpan { start, end })
                        .collect(),
                    field: String::new(),
                })
                .collect(),
        })
        .collect();
    Ok((hits, stage_columns_known))
}

struct FlatRun<'a> {
    node: &'a NodeServiceImpl,
    req: &'a Bm25QueryRequest,
    stages: Vec<ScoreStageSpec>,
    regions: Vec<crate::geo::GeoRegion>,
    params: Bm25Params,
    stats: bm25::CorpusStats,
}

impl<'a> FlatRun<'a> {
    fn new(node: &'a NodeServiceImpl, request: &'a Bm25QueryRequest) -> Result<Self, Status> {
        let stages = parse_score_stages(&request.score_stages)?;
        validate_range_facet_fields(&request.range_facet_fields)?;
        let regions = validate_geo_filters(&request.geo_filters)?;
        if let Some(filter) = request.filter.as_ref() {
            crate::filter::validate_filter(filter)?;
        }
        let params = params_from(request.k1, request.b)?;
        let stats = bm25::CorpusStats {
            doc_count: request.global_doc_count,
            total_doc_length: request.global_total_doc_length,
            dfs: request.global_doc_frequencies.clone(),
        };
        if request.terms.len() != stats.dfs.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        Ok(Self {
            node,
            req: request,
            stages,
            regions,
            params,
            stats,
        })
    }
}

/// The raw floor a request's `min_score` asks for: none at 0.
fn min_score_floor(min_score: f32) -> f64 {
    if min_score == 0.0 {
        f64::NEG_INFINITY
    } else {
        f64::from(min_score)
    }
}

fn flat_index<'a>(store: &'a Bm25Shard, masked: Option<&'a dyn Bm25Index>) -> &'a dyn Bm25Index {
    masked.unwrap_or_else(|| store.as_index())
}

struct FlatView<'a> {
    store: &'a Bm25Shard,
    index: &'a dyn Bm25Index,
    filter: Option<&'a crate::filter::DocFilter<'a>>,
    projections: &'a [crate::values::ResolvedValue],
}
