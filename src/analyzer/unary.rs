//! Analysis of one document through whichever backend an address names.

use tonic::Status;

use super::native::analyze_document_native_layers;
#[cfg(not(feature = "net"))]
use super::status::sidecar_needs_net;
use super::turbo::analyze_document_turbo;
use super::{AnalysisBackend, RemoteCallOptions, SessionLayers, MAX_TEXT_BYTES};
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Refuses a document no backend accepts: empty, or past the byte cap.
pub(super) fn check_document_text(text: &str) -> Result<(), Status> {
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
    Ok(())
}

/// Analyze `text` into an [`AnalyzedDoc`] (term, tf, original-text UTF-8 offsets,
/// and document length) using the configured backend at `addr`.
pub async fn analyze_document(
    addr: &str,
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    analyze_document_with_options(
        addr,
        text,
        spec,
        SessionLayers::default(),
        &RemoteCallOptions {
            interactive: true,
            ..Default::default()
        },
    )
    .await
}

/// Analyze `text` with explicit [`SessionLayers`] and [`RemoteCallOptions`]
/// (propagating mTLS, bearer token, priority lane, and RPC timeout).
pub async fn analyze_document_with_options(
    addr: &str,
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
    options: &RemoteCallOptions,
) -> Result<AnalyzedDoc, Status> {
    match AnalysisBackend::parse(addr)? {
        AnalysisBackend::Native => return analyze_document_native_layers(text, spec, layers),
        AnalysisBackend::Turbo => return analyze_document_turbo(text, spec, layers),
        AnalysisBackend::Sidecar(_) => {}
    }
    check_document_text(text)?;
    #[cfg(not(feature = "net"))]
    {
        let _ = (spec, layers, options);
        Err(sidecar_needs_net(addr))
    }
    #[cfg(feature = "net")]
    {
        super::remote::analyze_document_remote(addr, text, spec, layers, options).await
    }
}
