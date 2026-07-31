//! Client for the OpenNLP analysis sidecar (vendored proto
//! `ai.pipestream.opennlp.analysis.v1`).
//!
//! This is the ONLY analysis entry point: turbovec-search deliberately has
//! no tokenizer/stemmer/normalizer of its own — text in, term vectors out,
//! offsets always in original-text coordinates.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Status, Streaming};

use crate::pb::analysis::analysis_service_client::AnalysisServiceClient;
use crate::pb::analysis::{
    analyze_stream_request, analyze_stream_response, AnalysisOptions, AnalyzeRequest,
    AnalyzeResponse, AnalyzeStreamDoc, AnalyzeStreamRequest, AnalyzeStreamResponse,
    TermVectorOptions,
};
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Matches the sidecar's default text size cap.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Shared h2 channel to a sidecar address. tonic channels multiplex
/// concurrent calls over one connection and are cheap to clone; opening
/// a fresh TCP+h2 connection per Analyze (the previous behavior) buried
/// the sidecar under connection churn — its listener died after ~28k
/// rapid-fire calls while the process stayed alive.
pub fn shared_channel(addr: &str) -> Result<Channel, Status> {
    static CHANNELS: OnceLock<Mutex<HashMap<String, Channel>>> = OnceLock::new();
    let map = CHANNELS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(ch) = map.lock().expect("channel map poisoned").get(addr) {
        return Ok(ch.clone());
    }
    // connect_lazy defers the handshake to the first RPC, so this never
    // blocks the caller; tonic reconnects inside the channel on failure.
    let ch = Channel::from_shared(addr.to_string())
        .map_err(|e| Status::invalid_argument(format!("bad sidecar address {addr:?}: {e}")))?
        .connect_lazy();
    map.lock()
        .expect("channel map poisoned")
        .insert(addr.to_string(), ch.clone());
    Ok(ch)
}

/// Analyze `text` into an [`AnalyzedDoc`] (term, tf, original-text offsets,
/// and document length) using the sidecar at `addr`.
///
/// `spec` maps straight onto the sidecar's `AnalysisOptions`: term vectors
/// are always requested (FULL mode with occurrence offsets unless the spec
/// overrides), everything else defaults. INVALID_ARGUMENT for empty or
/// oversized text; UNAVAILABLE when the sidecar cannot be reached (the
/// shared channel connects lazily, so transport failures surface from the
/// call itself, not from client construction).
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
    let request = AnalyzeRequest {
        text: text.to_string(),
        options: Some(analysis_options(spec)),
    };
    let mut client = client(addr)?;
    // Raw Status passthrough: transport failures keep tonic's Unavailable
    // (the channel connects lazily, so "sidecar down" surfaces HERE, not
    // at client construction), server errors keep their own codes.
    let response = client.analyze(request).await?.into_inner();
    Ok(analyzed_from(response))
}

fn client(addr: &str) -> Result<AnalysisServiceClient<Channel>, Status> {
    Ok(AnalysisServiceClient::new(shared_channel(addr)?)
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
}

/// Maps `spec` straight onto the sidecar's `AnalysisOptions`: term vectors
/// are always requested (FULL mode with occurrence offsets unless the spec
/// overrides), everything else defaults.
fn analysis_options(spec: Option<&AnalysisSpec>) -> AnalysisOptions {
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
    AnalysisOptions {
        tokenizer,
        stemmer,
        term_vectors: Some(TermVectorOptions {
            enabled: true,
            mode,
            rungs,
            source,
        }),
        ..Default::default()
    }
}

/// Folds a response's term vectors into an [`AnalyzedDoc`] (term, tf,
/// original-text offsets, and document length).
fn analyzed_from(response: AnalyzeResponse) -> AnalyzedDoc {
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
    doc
}

/// Client-side submission buffer of an [`AnalyzeStream`]. Pacing is the
/// server's job (it grants transport credit from its worker capacity);
/// this only bounds local queuing before `submit` awaits.
const SUBMIT_BUFFER: usize = 32;

/// A cloneable submission handle for an open [`AnalyzeStream`].
#[derive(Clone)]
pub struct AnalyzeSubmit {
    requests: tokio::sync::mpsc::Sender<AnalyzeStreamRequest>,
}

impl AnalyzeSubmit {
    /// Queue one document, tagged with a caller-chosen sequence that the
    /// matching result echoes. Awaits only when the local buffer is full,
    /// which means the server has not granted credit yet: the await IS
    /// the backpressure. UNAVAILABLE when the stream is gone.
    pub async fn submit(&self, sequence: u64, text: &str) -> Result<(), Status> {
        self.requests
            .send(AnalyzeStreamRequest {
                msg: Some(analyze_stream_request::Msg::Doc(AnalyzeStreamDoc {
                    sequence,
                    text: text.to_string(),
                })),
            })
            .await
            .map_err(|_| Status::unavailable("analysis stream closed"))
    }
}

/// One AnalyzeStream call: many documents over one bidi RPC for one
/// analysis spec, paced end to end by the sidecar's server-side flow
/// control. Results arrive in COMPLETION order, tagged with the
/// submitted sequence; callers that need arrival order reorder.
pub struct AnalyzeStream {
    submit: Option<AnalyzeSubmit>,
    responses: Streaming<AnalyzeStreamResponse>,
}

impl AnalyzeStream {
    /// Opens a stream and sends its options message. UNIMPLEMENTED means
    /// the sidecar predates the RPC: fall back to unary
    /// [`analyze_document`] calls.
    ///
    /// The await resolves on call ACCEPTANCE, not on any result: the
    /// sidecar sends response headers eagerly (its
    /// EagerHeadersInterceptor, pinned by its own test) precisely so
    /// open-then-submit cannot deadlock on a first result that would
    /// only exist after the first submission.
    pub async fn open(addr: &str, spec: Option<&AnalysisSpec>) -> Result<Self, Status> {
        let mut client = client(addr)?;
        let (requests, feed) = tokio::sync::mpsc::channel(SUBMIT_BUFFER);
        requests
            .try_send(AnalyzeStreamRequest {
                msg: Some(analyze_stream_request::Msg::Options(analysis_options(spec))),
            })
            .expect("fresh channel has capacity");
        let responses = client
            .analyze_stream(ReceiverStream::new(feed))
            .await?
            .into_inner();
        Ok(Self {
            submit: Some(AnalyzeSubmit { requests }),
            responses,
        })
    }

    /// A cloneable submission handle, so submission can race result
    /// consumption in a `select` without borrowing the session twice.
    pub fn submitter(&self) -> AnalyzeSubmit {
        self.submit
            .clone()
            .expect("finished stream has no submitter")
    }

    /// Half-close the submission side. Once every [`submitter`] clone
    /// drops too, the server drains in-flight documents and completes.
    ///
    /// [`submitter`]: Self::submitter
    pub fn finish(&mut self) {
        self.submit = None;
    }

    /// The next per-document result, in completion order. `Ok(None)` is
    /// normal completion (after [`finish`](Self::finish)); the outer
    /// error is the stream itself failing, an inner error is one
    /// document failing on its own while the stream lives on.
    pub async fn next(&mut self) -> Result<Option<(u64, Result<AnalyzedDoc, Status>)>, Status> {
        match self.responses.message().await? {
            Some(response) => {
                let sequence = response.sequence;
                let result = match response.result {
                    Some(analyze_stream_response::Result::Ok(ok)) => Ok(analyzed_from(ok)),
                    Some(analyze_stream_response::Result::Error(error)) => {
                        Err(Status::new(tonic::Code::from(error.code), error.message))
                    }
                    None => Err(Status::internal(
                        "stream response carries neither ok nor error",
                    )),
                };
                Ok(Some((sequence, result)))
            }
            None => Ok(None),
        }
    }
}

/// Analyze a batch through one [`AnalyzeStream`] per distinct spec
/// (almost always exactly one), returning results in input order. Falls
/// back to concurrent unary calls against a sidecar that predates the
/// RPC. Any per-document failure fails the whole batch, the contract the
/// reshard replay tools rely on.
pub async fn analyze_batch(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>)],
) -> Result<Vec<AnalyzedDoc>, Status> {
    let mut out: Vec<Option<AnalyzedDoc>> = Vec::new();
    out.resize_with(docs.len(), || None);
    let receive = |item: Option<(u64, Result<AnalyzedDoc, Status>)>,
                       out: &mut Vec<Option<AnalyzedDoc>>|
     -> Result<(), Status> {
        let Some((sequence, result)) = item else {
            return Err(Status::internal(
                "analysis stream completed with documents unanswered",
            ));
        };
        let slot = out
            .get_mut(sequence as usize)
            .ok_or_else(|| Status::internal(format!("unknown result sequence {sequence}")))?;
        *slot = Some(result?);
        Ok(())
    };
    // Group indices by spec, preserving first-seen order; the global doc
    // index is the sequence, so results land in their input slots no
    // matter which group answered.
    let mut groups: Vec<(Option<&AnalysisSpec>, Vec<usize>)> = Vec::new();
    for (i, (_, spec)) in docs.iter().enumerate() {
        match groups.iter_mut().find(|(s, _)| *s == *spec) {
            Some((_, indices)) => indices.push(i),
            None => groups.push((*spec, vec![i])),
        }
    }
    for (spec, indices) in groups {
        let mut session = match AnalyzeStream::open(addr, spec).await {
            Ok(session) => session,
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                return analyze_batch_unary(addr, docs).await;
            }
            Err(status) => return Err(status),
        };
        let submit = session.submitter();
        let mut submitted = 0usize;
        let mut received = 0usize;
        while submitted < indices.len() {
            let i = indices[submitted];
            tokio::select! {
                sent = submit.submit(i as u64, docs[i].0) => {
                    sent?;
                    submitted += 1;
                }
                result = session.next() => {
                    receive(result?, &mut out)?;
                    received += 1;
                }
            }
        }
        drop(submit);
        session.finish();
        while received < indices.len() {
            receive(session.next().await?, &mut out)?;
            received += 1;
        }
    }
    Ok(out
        .into_iter()
        .map(|slot| slot.expect("every input index received exactly one result"))
        .collect())
}

/// The pre-stream behavior of the replay tools: one unary call per
/// document, fanned out concurrently over the shared channel.
async fn analyze_batch_unary(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>)],
) -> Result<Vec<AnalyzedDoc>, Status> {
    let tasks: Vec<_> = docs
        .iter()
        .map(|(text, spec)| {
            let addr = addr.to_string();
            let text = text.to_string();
            let spec = spec.cloned();
            tokio::spawn(async move { analyze_document(&addr, &text, spec.as_ref()).await })
        })
        .collect();
    let mut out = Vec::with_capacity(tasks.len());
    for task in tasks {
        out.push(
            task.await
                .map_err(|e| Status::internal(format!("analysis task failed: {e}")))??,
        );
    }
    Ok(out)
}
