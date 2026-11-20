//! Quantized SPLADE scoring with live-doc, filter, and visibility admission.

use crate::node::*;

/// Request-scoped facts every ScoreSplade response echoes.
struct SpladeEcho {
    filter_columns_known: Vec<bool>,
    stats_epoch: u64,
    stats_incarnation: Vec<u8>,
    visibility_fingerprint: Vec<u8>,
    visibility_columns_known: Vec<bool>,
}

impl SpladeEcho {
    /// Hold the request to the shard's statistics generation and capture what
    /// the response echoes.
    fn admit(
        guard: &ShardState,
        req: &crate::pb::ScoreSpladeRequest,
        scope: &crate::visibility::VisibilityScope,
        visibility: Option<&crate::pb::FilterExpr>,
    ) -> Result<Self, Status> {
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let (_, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], req.filter.as_ref());
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], visibility);
        Ok(Self {
            filter_columns_known,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        })
    }

    /// The answer of a shard that scored nothing.
    fn unscored(self, field_known: bool, initial_floor: f32) -> crate::pb::ScoreSpladeResponse {
        crate::pb::ScoreSpladeResponse {
            hits: Vec::new(),
            filter_columns_known: self.filter_columns_known,
            field_known,
            blocks_visited: 0,
            blocks_skipped: 0,
            postings_scored: 0,
            final_floor: initial_floor.max(0.0),
            stats_epoch: self.stats_epoch,
            stats_incarnation: self.stats_incarnation,
            visibility_fingerprint: self.visibility_fingerprint,
            visibility_columns_known: self.visibility_columns_known,
        }
    }
}

fn splade_field(req: &crate::pb::ScoreSpladeRequest) -> &str {
    if req.field.is_empty() {
        "body"
    } else {
        req.field.as_str()
    }
}

impl NodeServiceImpl {
    pub(in crate::node) fn checked_splade_k(&self, k: usize) -> Result<u32, Status> {
        if k > self.config.max_k as usize {
            return Err(Status::invalid_argument(format!(
                "k={k} exceeds this node's ScoreSplade max_k={}; lower k or raise --max-k",
                self.config.max_k
            )));
        }
        u32::try_from(k).map_err(|_| Status::invalid_argument("SPLADE k exceeds the wire range"))
    }

    /// Score quantized SPLADE 128-doc Block-Max postings on `field` with
    /// live-doc and CEL filter admission.
    pub fn score_splade_field(
        &self,
        field: &str,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let k = self.checked_splade_k(k)?;
        let resp = self.run_score_splade(crate::pb::ScoreSpladeRequest {
            field: field.to_string(),
            query_weights: query_weights
                .iter()
                .map(|(term, weight)| crate::pb::SpladeTermWeight {
                    term: term.clone(),
                    weight: *weight,
                })
                .collect(),
            query_l1_slack,
            k,
            filter: filter.cloned(),
            initial_floor: 0.0,
            visibility: None,
            expected_stats_epoch: 0,
            expected_stats_incarnation: Vec::new(),
        })?;
        Ok(resp.hits.into_iter().map(|h| (h.doc_id, h.score)).collect())
    }

    /// The admitted-row mask of one filter tree; `None` when there is no tree.
    fn splade_allow(
        &self,
        store: &Bm25Shard,
        rows: usize,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<Option<crate::boolean_bits::Bits>, Status> {
        if filter.is_none() {
            return Ok(None);
        }
        let (_, allow, _) = resolve_shard_filters(
            Some(store),
            None,
            rows,
            &[],
            &[],
            filter,
            self.knobs.segment_pruning(),
            None,
        )?;
        Ok(allow)
    }

    pub(crate) fn run_score_splade(
        &self,
        req: crate::pb::ScoreSpladeRequest,
    ) -> Result<crate::pb::ScoreSpladeResponse, Status> {
        self.checked_splade_k(req.k as usize)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let visibility = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let field = splade_field(&req);
        let guard = read_shard(&self.state);
        let echo = SpladeEcho::admit(&guard, &req, &scope, visibility)?;
        let Some(store) = guard.bm25.as_ref() else {
            return Ok(echo.unscored(false, req.initial_floor));
        };
        if store.as_index().is_none() {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let Some(fi) = store.field_index(field) else {
            return Ok(echo.unscored(false, req.initial_floor));
        };
        let index = store.field_view(fi).expect("searchability checked above");
        if !index.has_splade() {
            return Ok(echo.unscored(true, req.initial_floor));
        }
        let rows = store.next_doc_id() as usize;
        let allow = self.splade_allow(store, rows, req.filter.as_ref())?;
        let vis_allow = self.splade_allow(store, rows, visibility)?;
        let weights: Vec<(String, f32)> = req
            .query_weights
            .into_iter()
            .map(|tw| (tw.term, tw.weight))
            .collect();
        let admitted = |doc: u32| {
            !guard.live_docs.is_deleted(doc as usize)
                && allow.as_ref().is_none_or(|mask| mask.test(doc as usize))
                && vis_allow
                    .as_ref()
                    .is_none_or(|mask| mask.test(doc as usize))
        };
        let k = req.k as usize;
        let initial_floor = req.initial_floor.max(0.0);
        let (raw_hits, final_floor, stats) = index.score_splade_with_floor(
            &weights,
            req.query_l1_slack,
            initial_floor,
            k,
            &admitted,
        );
        let offset = self.config.slot_offset;
        let hits: Vec<crate::pb::SpladeHit> = raw_hits
            .into_iter()
            .map(|(doc_id, score)| crate::pb::SpladeHit {
                doc_id: offset + u64::from(doc_id),
                score,
            })
            .collect();
        Ok(crate::pb::ScoreSpladeResponse {
            hits,
            filter_columns_known: echo.filter_columns_known,
            field_known: true,
            blocks_visited: stats.blocks_visited,
            blocks_skipped: stats.blocks_skipped,
            postings_scored: stats.postings_scored,
            final_floor,
            stats_epoch: echo.stats_epoch,
            stats_incarnation: echo.stats_incarnation,
            visibility_fingerprint: echo.visibility_fingerprint,
            visibility_columns_known: echo.visibility_columns_known,
        })
    }
}
