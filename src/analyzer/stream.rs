//! The streaming bulk analysis session: submission handles, the response
//! side, and the per-document result loop.

#[cfg(feature = "net")]
use std::time::Duration;

#[cfg(feature = "net")]
use tonic::transport::Channel;
use tonic::Status;
#[cfg(feature = "net")]
use tonic::Streaming;
#[cfg(feature = "net")]
use turbo_client::proto::result::analysis_service_client::AnalysisServiceClient;
#[cfg(feature = "net")]
use turbo_client::proto::result::{
    analyze_stream_request, analyze_stream_response, AnalyzeDocument, AnalyzeOutput,
    AnalyzeStreamRequest, AnalyzeStreamResponse, OffsetMode,
};

#[cfg(feature = "net")]
use super::remote::{decode_graph_response, RemoteLink, SessionRequest};
#[cfg(feature = "net")]
use super::status::inspect_remote_status;
#[cfg(feature = "net")]
use super::wire;
use super::{RemoteCallOptions, SessionLayers};
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;
use crate::vocab::VocabularyListener;

/// Reports one analyzed document's body terms and surface tokens to the
/// vocabulary listener, when the session has one.
pub(super) fn feed_vocabulary(
    vocab: Option<&std::sync::Arc<VocabularyListener>>,
    doc: &AnalyzedDoc,
    surface_tokens: &[String],
) {
    if let Some(vocab) = vocab {
        let body = &doc.fields[0];
        vocab.feed(
            body.terms
                .iter()
                .map(|(term, frequency, _)| (term.as_str(), i64::from(*frequency))),
            surface_tokens.iter().map(String::as_str),
        );
    }
}

/// Client-side submission buffer of an [`AnalyzeStream`].
pub(super) const SUBMIT_BUFFER: usize = 32;

/// Cloneable submission handle for an open [`AnalyzeStream`].
#[derive(Clone)]
pub struct AnalyzeSubmit {
    pub(super) requests: AnalyzeRequests,
}

#[derive(Clone)]
pub(super) enum AnalyzeRequests {
    #[cfg(feature = "net")]
    Remote {
        tx: tokio::sync::mpsc::Sender<AnalyzeStreamRequest>,
        binding: turbo_client::proto::result::InputBinding,
    },
    InProcess(tokio::sync::mpsc::Sender<InProcessRequest>),
}

pub(super) struct InProcessRequest {
    pub(super) sequence: u64,
    pub(super) text: String,
}

pub(super) struct InProcessResponse {
    pub(super) sequence: u64,
    pub(super) result: Result<InProcessItem, Status>,
}

pub(super) struct InProcessItem {
    pub(super) doc: AnalyzedDoc,
    pub(super) surface_tokens: Vec<String>,
}

impl AnalyzeSubmit {
    /// Queue one document tagged with `sequence`.
    pub async fn submit(&self, sequence: u64, text: &str) -> Result<(), Status> {
        match &self.requests {
            #[cfg(feature = "net")]
            AnalyzeRequests::Remote { tx, binding } => tx
                .send(AnalyzeStreamRequest {
                    kind: Some(analyze_stream_request::Kind::Document(AnalyzeDocument {
                        id: sequence.to_string(),
                        model: String::new(),
                        text: text.as_bytes().to_vec(),
                        options: None,
                        output: AnalyzeOutput::DocumentGraph as i32,
                        offsets: OffsetMode::Utf8 as i32,
                        binding: Some(binding.clone()),
                        vocabulary: None,
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("analysis stream closed")),
            AnalyzeRequests::InProcess(requests) => requests
                .send(InProcessRequest {
                    sequence,
                    text: text.to_string(),
                })
                .await
                .map_err(|_| Status::unavailable("in-process analysis stream closed")),
        }
    }
}

/// Streaming bulk document analysis session (`turbo.result.v1.AnalysisService.AnalyzeStream`
/// or in-process `native` / `turbo` worker stream).
pub struct AnalyzeStream {
    pub(super) submit: Option<AnalyzeSubmit>,
    pub(super) responses: AnalyzeResponses,
    pub(super) vocab: Option<std::sync::Arc<VocabularyListener>>,
    pub(super) spec: Option<AnalysisSpec>,
    pub(super) layers: SessionLayers,
}

#[cfg(feature = "net")]
pub(super) struct RemoteStreamState {
    pub(super) addr: String,
    pub(super) remote: turbo_client::RemoteClient,
    pub(super) client: AnalysisServiceClient<Channel>,
    pub(super) timeout: Option<Duration>,
    pub(super) responses: Streaming<AnalyzeStreamResponse>,
}

pub(super) enum AnalyzeResponses {
    #[cfg(feature = "net")]
    Remote(Box<RemoteStreamState>),
    InProcess(tokio::sync::mpsc::Receiver<InProcessResponse>),
}

#[cfg(feature = "net")]
impl RemoteStreamState {
    /// The next answered document from the remote stream, decoded for
    /// `session`.
    async fn next(
        &mut self,
        session: SessionRequest<'_>,
    ) -> Result<Option<(u64, Result<AnalyzedDoc, Status>)>, Status> {
        let Some(response) = self
            .responses
            .message()
            .await
            .map_err(|s| inspect_remote_status(&self.addr, s))?
        else {
            return Ok(None);
        };
        let sequence = response.id.parse::<u64>().map_err(|_| {
            Status::internal(format!(
                "analysis stream returned non-numeric correlation id {:?}",
                response.id
            ))
        })?;
        let result = match response.outcome {
            Some(analyze_stream_response::Outcome::DocumentGraph(graph)) => {
                let link = RemoteLink {
                    addr: &self.addr,
                    remote: &self.remote,
                    client: &mut self.client,
                    timeout: self.timeout,
                };
                decode_graph_response(link, session, graph).await
            }
            Some(analyze_stream_response::Outcome::Failure(failure)) => {
                Err(wire::status_from_analyze_failure(failure))
            }
            Some(_) => Err(Status::internal(
                "analysis stream returned unexpected non-graph outcome",
            )),
            None => Err(Status::internal("stream response carries no outcome")),
        };
        Ok(Some((sequence, result)))
    }
}

impl AnalyzeStream {
    /// Opens an [`AnalyzeStream`] session with default [`SessionLayers`].
    pub async fn open(addr: &str, spec: Option<&AnalysisSpec>) -> Result<Self, Status> {
        Self::open_with_vocab(addr, spec, None, SessionLayers::default()).await
    }

    /// Opens an [`AnalyzeStream`] session with optional [`crate::vocab::VocabularyListener`]
    /// and [`SessionLayers`], preflighting remote server capabilities when needed.
    pub async fn open_with_vocab(
        addr: &str,
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        Self::open_with_options(
            addr,
            spec,
            vocab,
            layers,
            &RemoteCallOptions {
                interactive: false,
                ..Default::default()
            },
        )
        .await
    }

    /// Returns a cloneable submission handle.
    pub fn submitter(&self) -> AnalyzeSubmit {
        self.submit
            .clone()
            .expect("finished stream has no submitter")
    }

    pub(super) async fn run_to_completion(
        mut self,
        items: &[(u64, &str)],
    ) -> Result<Vec<(u64, AnalyzedDoc)>, Status> {
        let mut out = Vec::with_capacity(items.len());
        let submit = self.submitter();
        let (mut submitted, mut received) = (0usize, 0usize);
        let take = |item: Option<(u64, Result<AnalyzedDoc, Status>)>,
                    out: &mut Vec<(u64, AnalyzedDoc)>|
         -> Result<(), Status> {
            let Some((sequence, result)) = item else {
                return Err(Status::internal(
                    "analysis stream completed with documents unanswered",
                ));
            };
            out.push((sequence, result?));
            Ok(())
        };
        while submitted < items.len() {
            let (sequence, text) = items[submitted];
            tokio::select! {
                sent = submit.submit(sequence, text) => {
                    sent?;
                    submitted += 1;
                }
                result = self.next() => {
                    take(result?, &mut out)?;
                    received += 1;
                }
            }
        }
        drop(submit);
        self.finish();
        while received < items.len() {
            take(self.next().await?, &mut out)?;
            received += 1;
        }
        self.drain_to_end().await?;
        Ok(out)
    }

    /// Reads the server's clean end-of-stream so trailers are drained without
    /// emitting HTTP/2 `RST_STREAM`.
    pub async fn drain_to_end(&mut self) -> Result<(), Status> {
        match self.next().await? {
            None => Ok(()),
            Some((sequence, _)) => Err(Status::internal(format!(
                "analysis stream answered sequence {sequence} after every submitted document \
                 was answered"
            ))),
        }
    }

    /// Half-closes the client submission side.
    pub fn finish(&mut self) {
        self.submit = None;
    }

    /// Next per-document result in completion order.
    pub async fn next(&mut self) -> Result<Option<(u64, Result<AnalyzedDoc, Status>)>, Status> {
        match &mut self.responses {
            #[cfg(feature = "net")]
            AnalyzeResponses::Remote(state) => {
                let session = SessionRequest {
                    spec: self.spec.as_ref(),
                    layers: self.layers,
                    vocab: self.vocab.as_ref(),
                };
                state.next(session).await
            }
            AnalyzeResponses::InProcess(responses) => {
                // Only the remote arm reads these; touching them here keeps
                // a build without `net` free of unread fields.
                let _ = &self.spec;
                let _ = &self.layers;
                match responses.recv().await {
                    Some(response) => {
                        let result = response.result.map(|item| {
                            feed_vocabulary(self.vocab.as_ref(), &item.doc, &item.surface_tokens);
                            item.doc
                        });
                        Ok(Some((response.sequence, result)))
                    }
                    None => Ok(None),
                }
            }
        }
    }
}
