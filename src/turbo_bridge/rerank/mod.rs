//! Stage-2 calibrated `RerankAndHighlight` integration.
//!
//! Reranks a top-`C` Stage-1 shortlist into calibrated `[0.0, 1.0]` relevance
//! probabilities while attributing semantic highlight spans (`QueryHit.snippets`)
//! in original `ViewId::SOURCE` UTF-8 byte coordinates (with optional
//! `DocLineage.span_start` parent-document offset composition), populating
//! snippets on pure dense (`VectorQuery`) and hybrid hits even when zero
//! lexical postings matched.

mod deterministic;
#[cfg(feature = "local-core")]
mod local;
#[cfg(feature = "net")]
mod remote;
mod snippet;
#[cfg(test)]
mod tests;

#[cfg(feature = "local-core")]
use std::sync::Arc;

use tonic::Status;

use crate::pb::Snippet;
use crate::turbo_bridge::remaining_deadline_duration;

/// Execution backend for Stage-2 calibrated reranking and semantic highlighting.
#[derive(Clone)]
pub enum RerankBackend {
    /// Remote `turbo-kserve` `AnalysisService::RerankAndHighlight` execution over
    /// runtime-scoped pooled HTTP/2 with `RemotePriority::Interactive`, bearer
    /// auth / mTLS, and `finish_deadline_us` propagation.
    #[cfg(feature = "net")]
    Remote {
        /// Pooled remote `turbo-kserve` client.
        client: Box<turbo_client::RemoteClient>,
        /// `TURBO_TASK_RERANK` bundle name served on `turbo-kserve`.
        rerank_model: String,
        /// Optional `TURBO_TASK_EMBED` bundle name for pooled/MaxSim highlights.
        embed_model: String,
        /// Optional `TURBO_TASK_EXTRACTIVE_QA` bundle name for extractive QA spans.
        qa_model: String,
        /// Temperature calibration factor (`> 0.0`, default `1.0`).
        temperature: f64,
    },
    /// In-process `libturbo` `TURBO_TASK_RERANK` bundle (`feature = "local-core"`).
    #[cfg(feature = "local-core")]
    Local {
        /// Thread-safe local rerank + calibrator session.
        engine: Arc<turbo_client::LocalRerankEngine>,
    },
    /// Deterministic calibrated lexical-semantic overlap scorer (`Platt`
    /// logistic mapping into `[0.0, 1.0]`) with sentence/window span attribution
    /// for embedded / CPU-only deployments and deterministic integration tests.
    DeterministicCalibrated {
        /// Logistic Platt slope (`a` in `sigmoid(a * z + b)`).
        platt_slope: f64,
        /// Logistic Platt intercept (`b` in `sigmoid(a * z + b)`).
        platt_intercept: f64,
    },
}

impl std::fmt::Debug for RerankBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "net")]
            Self::Remote { rerank_model, .. } => f
                .debug_struct("Remote")
                .field("rerank_model", rerank_model)
                .finish(),
            #[cfg(feature = "local-core")]
            Self::Local { .. } => f.debug_struct("Local").finish(),
            Self::DeterministicCalibrated {
                platt_slope,
                platt_intercept,
            } => f
                .debug_struct("DeterministicCalibrated")
                .field("platt_slope", platt_slope)
                .field("platt_intercept", platt_intercept)
                .finish(),
        }
    }
}

/// Stage-2 reranking and semantic highlight policy attached to a coordinator
/// or query execution plan.
#[derive(Clone, Debug)]
pub struct RerankSpec {
    /// Maximum Stage-1 shortlist depth `C` sent to the cross-encoder (`>= 1`).
    pub shortlist_size: usize,
    /// Top-`K` reranked candidates for which semantic highlight spans are computed.
    pub highlight_top_k: usize,
    /// Maximum non-overlapping semantic snippets emitted per candidate.
    pub max_snippets_per_candidate: usize,
    /// Stored text field name reported on emitted [`Snippet`] messages.
    pub field: String,
    /// ISO 639-3 sentence segmentation language code (e.g. `"eng"`).
    pub language: String,
    /// When `true`, adds `DocLineage.span_start` onto emitted snippet and
    /// highlight UTF-8 byte offsets so chunk hits project directly onto their
    /// parent document's coordinate space.
    pub compose_parent_lineage_offsets: bool,
    /// Optional explicit query string used when reranking a pure `VectorQuery`
    /// selection that carries only float coordinates on the wire.
    pub query_text_override: Option<String>,
    /// Underlying remote, local-bundle, or deterministic calibrated backend.
    pub backend: RerankBackend,
}

impl RerankSpec {
    /// Creates a Stage-2 rerank specification with sensible defaults (`C = 32`,
    /// `highlight_top_k = 10`, `max_snippets = 2`, `field = "body"`).
    pub fn new(backend: RerankBackend) -> Self {
        Self {
            shortlist_size: 32,
            highlight_top_k: 10,
            max_snippets_per_candidate: 2,
            field: "body".to_string(),
            language: "eng".to_string(),
            compose_parent_lineage_offsets: false,
            query_text_override: None,
            backend,
        }
    }
}

/// Candidate passage supplied to [`rerank_and_highlight_shortlist`].
#[derive(Clone, Debug)]
pub struct RerankCandidate {
    /// Document identifier (`QueryHit.id`).
    pub doc_id: u64,
    /// Stage-1 retrieval or fusion score prior to Stage-2 reranking.
    pub prior_score: f64,
    /// Stored original UTF-8 text of `RerankSpec::field`.
    pub text: String,
    /// Optional stored sentence spans `(start_utf8, end_utf8)` over `text`.
    pub sentence_spans: Vec<(u32, u32)>,
    /// Optional `DocLineage.span_start` offset within a parent document.
    pub lineage_span_start: u32,
}

/// Calibrated Stage-2 rerank output and semantic snippets for one candidate.
#[derive(Clone, Debug)]
pub struct RerankedHit {
    /// Zero-based index into the input `candidates` slice.
    pub input_index: usize,
    /// Document identifier (`QueryHit.id`).
    pub doc_id: u64,
    /// Uncalibrated cross-encoder logit.
    pub raw_logit: f32,
    /// Calibrated relevance probability strictly within `[0.0, 1.0]`.
    pub calibrated_probability: f64,
    /// Source-projected semantic highlight snippets (`QueryHit.snippets`).
    pub snippets: Vec<Snippet>,
}

/// Reranks up to `spec.shortlist_size` candidates in descending order of
/// calibrated `[0.0, 1.0]` relevance probability and attributes semantic
/// highlight snippets.
pub async fn rerank_and_highlight_shortlist(
    spec: &RerankSpec,
    query_text: &str,
    candidates: &[RerankCandidate],
    finish_deadline_us: u64,
) -> Result<Vec<RerankedHit>, Status> {
    if candidates.is_empty() || spec.shortlist_size == 0 {
        return Ok(Vec::new());
    }
    let limit = candidates.len().min(spec.shortlist_size);
    let shortlist = &candidates[..limit];
    let _timeout = remaining_deadline_duration(finish_deadline_us)?;

    match &spec.backend {
        #[cfg(feature = "net")]
        RerankBackend::Remote {
            client,
            rerank_model,
            embed_model,
            qa_model,
            temperature,
        } => {
            let call = remote::RemoteRerank {
                client,
                rerank_model,
                embed_model,
                qa_model,
                temperature: *temperature,
            };
            remote::rerank(spec, query_text, shortlist, &call, _timeout).await
        }
        #[cfg(feature = "local-core")]
        RerankBackend::Local { engine } => local::rerank(spec, query_text, shortlist, engine).await,
        RerankBackend::DeterministicCalibrated {
            platt_slope,
            platt_intercept,
        } => Ok(deterministic::rerank(
            spec,
            query_text,
            shortlist,
            *platt_slope,
            *platt_intercept,
        )),
    }
}
