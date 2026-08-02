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

/// Embed `text` through the sidecar's embedding model (Model2Vec-style,
/// `EmbeddingOptions` over sentences) into ONE query vector: sentence
/// embeddings are mean-pooled and L2-normalized, matching the
/// unit-normalized corpus vectors. Fails FAILED_PRECONDITION when the
/// sidecar has no embedding model loaded (its warning is passed
/// through).
pub async fn embed_text(addr: &str, text: &str) -> Result<Vec<f32>, Status> {
    use crate::pb::analysis::{embedding_options, EmbeddingOptions};
    if text.is_empty() {
        return Err(Status::invalid_argument("empty text"));
    }
    let request = AnalyzeRequest {
        text: text.to_string(),
        options: Some(AnalysisOptions {
            embeddings: Some(EmbeddingOptions {
                source: embedding_options::Source::Sentences as i32,
            }),
            ..Default::default()
        }),
    };
    let response = client(addr)?.analyze(request).await?.into_inner();
    if response.embeddings.is_empty() {
        return Err(Status::failed_precondition(format!(
            "sidecar returned no embeddings ({})",
            if response.warnings.is_empty() {
                "no warning given".to_string()
            } else {
                response.warnings.join("; ")
            }
        )));
    }
    let dim = response.embeddings[0].vector.len();
    let mut pooled = vec![0.0f64; dim];
    for chunk in &response.embeddings {
        if chunk.vector.len() != dim {
            return Err(Status::internal("sidecar embeddings disagree on dim"));
        }
        for (acc, &v) in pooled.iter_mut().zip(&chunk.vector) {
            *acc += f64::from(v);
        }
    }
    let n = response.embeddings.len() as f64;
    let norm = pooled.iter().map(|v| (v / n).powi(2)).sum::<f64>().sqrt();
    if norm == 0.0 {
        return Err(Status::failed_precondition("embedding pooled to zero"));
    }
    Ok(pooled.iter().map(|v| ((v / n) / norm) as f32).collect())
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

/// Folds a response's term vectors into a single-field (body)
/// [`AnalyzedDoc`] (term, tf, original-text offsets, and document
/// length).
fn analyzed_from(response: AnalyzeResponse) -> AnalyzedDoc {
    let mut terms = crate::postings::DocTerms::new();
    let mut length = 0u32;
    for tv in response.term_vectors {
        if tv.frequency <= 0 {
            continue;
        }
        let offsets = tv
            .occurrences
            .iter()
            .map(|s| (s.start.max(0) as u32, s.end.max(0) as u32))
            .collect();
        length += tv.frequency as u32;
        terms.push((tv.term, tv.frequency as u32, offsets));
    }
    AnalyzedDoc::body(terms, length)
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
    /// the sidecar predates the RPC and must be rebuilt; there is no
    /// unary fallback (see [`analyze_batch`] for why it was removed).
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

    /// Submit `indices` (as sequences) through this one stream and
    /// collect every result, racing submission against consumption so a
    /// full local buffer never stalls the drain.
    ///
    /// Extracted so the multi-stream path runs the SAME loop N times
    /// rather than a second implementation of it: the pacing here is
    /// subtle (the await in `submit` is the backpressure), and two
    /// versions of it would drift.
    async fn run_to_completion(
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
        Ok(out)
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

/// Analyze a batch through [`AnalyzeStream`], returning results in INPUT
/// order. Any per-document failure fails the whole batch, the contract
/// the reshard replay tools rely on.
///
/// One stream per distinct spec (almost always exactly one). For more
/// than one stream per spec see [`analyze_batch_streams`].
pub async fn analyze_batch(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>)],
) -> Result<Vec<AnalyzedDoc>, Status> {
    analyze_batch_streams(addr, docs, 1).await
}

/// [`analyze_batch`] over `streams` concurrent AnalyzeStreams per spec.
///
/// One stream is a pipeline, not a parallel: the sidecar paces it with
/// its own flow control, so a single stream can leave analysis workers
/// idle while it waits on the wire. Since analysis is the ceiling on
/// bulk ingest (shard parallelism is not), opening several lets the
/// sidecar work on several documents at once. The right number is a
/// property of the sidecar's worker pool, not of this client, which is
/// why it is a parameter rather than a constant.
///
/// Results are keyed by the caller's sequence and land in their input
/// slots, so the OUTPUT IS BYTE-IDENTICAL whatever `streams` is set to.
/// Analysis is a pure function of (text, spec); splitting the work
/// changes only who waits. That is pinned by test, because a throughput
/// knob that quietly perturbed term identity would corrupt an index
/// rather than slow one down.
///
/// `streams` is clamped to at least 1 and to the number of documents in
/// a group; more streams than documents would open connections that
/// immediately close.
pub async fn analyze_batch_streams(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>)],
    streams: usize,
) -> Result<Vec<AnalyzedDoc>, Status> {
    let mut out: Vec<Option<AnalyzedDoc>> = Vec::new();
    out.resize_with(docs.len(), || None);
    // Group indices by spec, preserving first-seen order; the global doc
    // index is the sequence, so results land in their input slots no
    // matter which group or which stream answered.
    let mut groups: Vec<(Option<&AnalysisSpec>, Vec<usize>)> = Vec::new();
    for (i, (_, spec)) in docs.iter().enumerate() {
        match groups.iter_mut().find(|(s, _)| *s == *spec) {
            Some((_, indices)) => indices.push(i),
            None => groups.push((*spec, vec![i])),
        }
    }
    for (spec, indices) in groups {
        let want = streams.max(1).min(indices.len());
        if want == 0 {
            continue;
        }
        // Contiguous chunks, not round-robin: consecutive documents in a
        // bulk replay tend to be similar in size, so contiguity keeps the
        // per-stream loads even without knowing anything about them.
        let per = indices.len().div_ceil(want);
        let chunks: Vec<Vec<(u64, &str)>> = indices
            .chunks(per)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|&i| (i as u64, docs[i].0))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut sessions = Vec::with_capacity(chunks.len());
        for _ in 0..chunks.len() {
            sessions.push(open_stream(addr, spec).await?);
        }
        // Drive every stream from this one task: the work being
        // overlapped is the sidecar's, and these futures only wait on
        // I/O. No spawn, so nothing has to be cloned to satisfy 'static.
        let mut running: Vec<_> = sessions
            .into_iter()
            .zip(&chunks)
            .map(|(session, chunk)| Box::pin(session.run_to_completion(chunk)))
            .collect();
        for done in join_all(&mut running).await {
            for (sequence, doc) in done? {
                let slot = out.get_mut(sequence as usize).ok_or_else(|| {
                    Status::internal(format!("unknown result sequence {sequence}"))
                })?;
                *slot = Some(doc);
            }
        }
    }
    Ok(out
        .into_iter()
        .map(|slot| slot.expect("every input index received exactly one result"))
        .collect())
}

/// Open one stream, naming a version skew rather than degrading.
///
/// No quiet downgrade to per-document unary calls. That fallback existed
/// and cost real debugging time: this is a BULK path (reshard and WAL
/// replay), and the unary transport opens one h2 stream per document, so
/// a sidecar predating AnalyzeStream GOAWAYs after ~70 of them. The
/// "fallback" did not degrade gracefully, it failed obscurely thousands
/// of documents later.
async fn open_stream(addr: &str, spec: Option<&AnalysisSpec>) -> Result<AnalyzeStream, Status> {
    match AnalyzeStream::open(addr, spec).await {
        Ok(session) => Ok(session),
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            Err(Status::failed_precondition(format!(
                "analysis sidecar at {addr} does not implement AnalyzeStream; \
                 it predates the RPC and must be rebuilt (./gradlew installDist \
                 in grpc-opennlp-analysis)"
            )))
        }
        Err(status) => Err(status),
    }
}

/// Await every future to completion, returning results in input order.
///
/// Hand-rolled because the crate does not depend on `futures`, and this
/// is the whole of what it would be used for: poll each in turn, yield
/// when none is ready. The waker is shared, so a wake from any one
/// re-polls all of them, which is correct if slightly eager for the
/// handful of streams this ever holds.
async fn join_all<F: std::future::Future>(futures: &mut [std::pin::Pin<Box<F>>]) -> Vec<F::Output> {
    let mut done: Vec<Option<F::Output>> = (0..futures.len()).map(|_| None).collect();
    let mut remaining = futures.len();
    std::future::poll_fn(|cx| {
        for (slot, future) in done.iter_mut().zip(futures.iter_mut()) {
            if slot.is_some() {
                continue;
            }
            if let std::task::Poll::Ready(value) = future.as_mut().poll(cx) {
                *slot = Some(value);
                remaining -= 1;
            }
        }
        if remaining == 0 {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    done.into_iter()
        .map(|slot| slot.expect("poll_fn returned only when every future completed"))
        .collect()
}
