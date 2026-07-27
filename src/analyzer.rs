//! Client for the OpenNLP analysis sidecar (vendored proto
//! `ai.pipestream.opennlp.analysis.v1`).
//!
//! This is the ONLY analysis entry point: turbovec-search deliberately has
//! no tokenizer/stemmer/normalizer of its own — text in, term vectors out,
//! offsets always in original-text coordinates.

use crate::pb::analysis::analysis_service_client::AnalysisServiceClient;
use crate::pb::analysis::{AnalysisOptions, AnalyzeRequest, TermVectorOptions};
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;
use tonic::Status;

/// Matches the sidecar's default text size cap.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Analyze `text` into an [`AnalyzedDoc`] (term, tf, original-text offsets,
/// and document length) using the sidecar at `addr`.
///
/// `spec` maps straight onto the sidecar's `AnalysisOptions`: term vectors
/// are always requested (FULL mode with occurrence offsets unless the spec
/// overrides), everything else defaults. INVALID_ARGUMENT for empty or
/// oversized text; UNAVAILABLE when the sidecar cannot be reached.
pub async fn analyze_document(
    addr: &str,
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    if text.is_empty() {
        return Err(Status::invalid_argument("empty document text"));
    }
    if text.len() > MAX_TEXT_BYTES {
        return Err(Status::invalid_argument(format!(
            "document of {} bytes exceeds the {}-byte cap",
            text.len(),
            MAX_TEXT_BYTES
        )));
    }
    let (mode, source, rungs, tokenizer, stemmer) = match spec {
        Some(s) => (
            s.term_vector_mode,
            s.term_vector_source,
            s.normalizer_rungs.clone(),
            s.tokenizer,
            s.stemmer,
        ),
        None => (0, 0, Vec::new(), 0, 0),
    };
    let request = AnalyzeRequest {
        text: text.to_string(),
        options: Some(AnalysisOptions {
            tokenizer,
            stemmer,
            term_vectors: Some(TermVectorOptions {
                enabled: true,
                mode,
                rungs,
                source,
            }),
            ..Default::default()
        }),
    };
    let mut client = AnalysisServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("analysis sidecar at {addr}: {e}")))?;
    let response = client
        .analyze(request)
        .await
        .map_err(|e| Status::internal(format!("analysis failed: {e}")))?
        .into_inner();

    let mut doc = AnalyzedDoc::default();
    for tv in response.term_vectors {
        if tv.frequency <= 0 {
            continue;
        }
        let offsets = tv
            .occurrences
            .iter()
            .map(|s| (s.start.max(0) as u32, s.end.max(0) as u32))
            .collect();
        doc.length += tv.frequency as u32;
        doc.terms.push((tv.term, tv.frequency as u32, offsets));
    }
    Ok(doc)
}
