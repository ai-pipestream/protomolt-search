//! Unary analysis against a remote `turbo-kserve` endpoint, and decoding of
//! the document graph such an endpoint answers with.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::Status;
use turbo_client::proto::result::analysis_service_client::AnalysisServiceClient;
use turbo_client::proto::result::{
    analyze_progressive_event, AnalyzePipelineRequest, DocumentGraphResponse, OffsetMode,
    PiiOutputOptions, ProjectSearchDocumentRequest,
};

use super::pool::remote_client_for;
use super::projection::analyzed_from_search_projection;
use super::spec::{
    body_spec, turbo_handshake, validate_dual_cased_spec, TERM_VECTOR_MODE_SCORING_ONLY,
};
use super::spec_convert::turbo_field_config;
use super::status::{client_err_to_status, inspect_remote_client_err, inspect_remote_status};
use super::stream::feed_vocabulary;
use super::turbo::{turbo_analysis_pass, turbo_field_to_postings};
use super::{wire, RemoteCallOptions, SessionLayers};
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;
use crate::vocab::VocabularyListener;

/// One dialed analysis endpoint: its address, the pooled client that
/// prepares requests for it, the service stub, and the per-RPC deadline.
pub(super) struct RemoteLink<'a> {
    pub(super) addr: &'a str,
    pub(super) remote: &'a turbo_client::RemoteClient,
    pub(super) client: &'a mut AnalysisServiceClient<Channel>,
    pub(super) timeout: Option<Duration>,
}

/// What one analysis session asked for, and the listener its terms feed.
#[derive(Clone, Copy)]
pub(super) struct SessionRequest<'a> {
    pub(super) spec: Option<&'a AnalysisSpec>,
    pub(super) layers: SessionLayers,
    pub(super) vocab: Option<&'a Arc<VocabularyListener>>,
}

/// The analysis service stub over `remote`'s pooled channel.
pub(super) fn analysis_client(
    remote: &turbo_client::RemoteClient,
) -> Result<AnalysisServiceClient<Channel>, Status> {
    let ch = remote
        .pool()
        .channel(remote.config())
        .map_err(client_err_to_status)?;
    Ok(AnalysisServiceClient::new(ch)
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
}

pub(super) async fn analyze_document_remote(
    addr: &str,
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
    options: &RemoteCallOptions,
) -> Result<AnalyzedDoc, Status> {
    let remote = remote_client_for(addr, options)?;
    let mut client = analysis_client(&remote)?;
    let session = wire::WireSessionOptions {
        spec: spec.cloned(),
        layers,
    };
    let payload = AnalyzePipelineRequest {
        doc_id: "0".to_string(),
        text: text.to_string(),
        offsets: OffsetMode::Utf8 as i32,
        profile: "default-v1".to_string(),
        pii_output: Some(PiiOutputOptions {
            full_text: true,
            ..Default::default()
        }),
        binding: Some(session.to_binding()),
        ..Default::default()
    };
    let req = remote
        .prepare_request(payload, Some(options.priority()), options.timeout)
        .map_err(client_err_to_status)?;
    let mut stream = client
        .analyze_document_progressive(req)
        .await
        .map_err(|s| inspect_remote_status(addr, s))?
        .into_inner();
    let mut graph_resp: Option<DocumentGraphResponse> = None;
    while let Some(ev) = stream
        .message()
        .await
        .map_err(|s| inspect_remote_status(addr, s))?
    {
        if let Some(analyze_progressive_event::Event::Complete(done)) = ev.event {
            graph_resp = Some(done);
        }
    }
    let done = graph_resp.ok_or_else(|| {
        Status::internal("AnalyzeDocumentProgressive stream ended without a Complete event")
    })?;
    let link = RemoteLink {
        addr,
        remote: &remote,
        client: &mut client,
        timeout: options.timeout,
    };
    let session = SessionRequest {
        spec,
        layers,
        vocab: None,
    };
    decode_graph_response(link, session, done).await
}

/// Turns one document graph response into an [`AnalyzedDoc`]: a mock graph
/// decodes directly, a real graph is projected by the endpoint, and a
/// response carrying only source text is analyzed in-process.
pub(super) async fn decode_graph_response(
    link: RemoteLink<'_>,
    session: SessionRequest<'_>,
    response: DocumentGraphResponse,
) -> Result<AnalyzedDoc, Status> {
    if response.graph.starts_with(wire::MOCK_GRAPH_MAGIC) {
        let mock = wire::MockAnalyzeResponse::decode_graph(&response.graph)?;
        if let Some(vocab) = session.vocab {
            vocab.feed(
                mock.term_vectors
                    .iter()
                    .map(|tv| (tv.term.as_str(), i64::from(tv.frequency))),
                mock.tokens.iter().map(|t| t.text.as_str()),
            );
        }
        return wire::analyzed_from_mock(mock, session.layers);
    }
    if !response.graph.is_empty() {
        return decode_projected_graph(link, session, response).await;
    }
    if response.source_text.is_empty() {
        return Err(Status::failed_precondition(
            "turbo-kserve returned an empty DocumentGraphResponse (check PII output disclosure)",
        ));
    }
    let out = turbo_analysis_pass(&response.source_text, session.spec, session.layers)?;
    feed_vocabulary(session.vocab, &out.doc, &out.surface_tokens);
    Ok(out.doc)
}

async fn decode_projected_graph(
    link: RemoteLink<'_>,
    session: SessionRequest<'_>,
    response: DocumentGraphResponse,
) -> Result<AnalyzedDoc, Status> {
    let SessionRequest {
        spec,
        layers,
        vocab,
    } = session;
    let wire_session = wire::WireSessionOptions {
        spec: spec.cloned(),
        layers,
    };
    let analyzer_config_digest = turbo_handshake(spec, None)
        .ok()
        .and_then(|hs| hs.analysis.map(|a| a.sha256.to_vec()))
        .unwrap_or_default();
    let proj_payload = ProjectSearchDocumentRequest {
        doc_id: String::new(),
        graph: response.graph.to_vec(),
        view: 0,
        offsets: OffsetMode::Utf8 as i32,
        pii_output: Some(PiiOutputOptions {
            full_text: true,
            ..Default::default()
        }),
        binding: Some(wire_session.to_binding()),
        analyzer_config_digest,
    };
    let prio = link.remote.config().default_priority;
    let proj_req = link
        .remote
        .prepare_request(proj_payload, Some(prio), link.timeout)
        .map_err(|e| inspect_remote_client_err(link.addr, e))?;
    let projected = link
        .client
        .project_search_document(proj_req)
        .await
        .map_err(|s| inspect_remote_status(link.addr, s))?
        .into_inner();
    let scoring_only = spec
        .map(|s| s.term_vector_mode == TERM_VECTOR_MODE_SCORING_ONLY)
        .unwrap_or(false);
    let (mut doc, surface_tokens) =
        analyzed_from_search_projection(projected, scoring_only, layers)?;
    if layers.dual_cased && doc.cased.is_none() && !response.source_text.is_empty() {
        validate_dual_cased_spec(spec)?;
        let effective_spec = spec.cloned().unwrap_or_else(body_spec);
        let cfg = turbo_field_config(Some(&effective_spec))?.cased_twin();
        let cased = turbo_client::LexicalAnalyzer::new(cfg)
            .analyze(&response.source_text)
            .map_err(client_err_to_status)?;
        doc.cased = Some(turbo_field_to_postings(cased, scoring_only));
    }
    feed_vocabulary(vocab, &doc, &surface_tokens);
    Ok(doc)
}
