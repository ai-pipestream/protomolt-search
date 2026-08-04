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

/// The analysis a body-text corpus is built with, and the ONLY spec that
/// may be used to query one.
///
/// Term identity is decided entirely inside the sidecar, so an index and
/// a query that disagree about this struct do not fail — they silently
/// score different terms. That has now cost this project twice: once
/// when a query went out unstemmed against a stemmed index, and once
/// when the v7 corpus was built under `SOURCE_STEMS` (below). Both times
/// the spec was written out by hand at the call site, and there were
/// thirteen such copies. There is now one, and callers take it from
/// here.
///
/// `SOURCE_NORMALIZED_STEMS` (3), not `SOURCE_STEMS` (2). The sidecar's
/// own proto calls SOURCE_STEMS "a trap for any corpus that is not
/// already lower case", and it is: stemmers operate on the surface form
/// and do not fold case, so capitalization survives into term identity.
/// Measured on the 86.6M-chunk court corpus built that way, `court`,
/// `Court` and `COURT` were three separate terms with df 36,113,172 /
/// 22,353,022 / 2,165,891 — a lowercase query reached 60% of them and
/// scored the term as far rarer than it is. Proper nouns fared worst
/// (`Dragon` df 4,571 vs `dragon` 508), so a search for "dungeons and
/// dragons" returned dragon-toy copyright suits and prison dungeons
/// while every Dungeons & Dragons opinion sat under the capitalized
/// terms, unreachable.
///
/// The char filters run before the stemmer under source 3. STRIP_INVISIBLE and
/// WHITESPACE are the sidecar's own defaults; FULL_CASE_FOLD is the one
/// that does the work here.
///
/// ACCENT_FOLD is here for the same reason FULL_CASE_FOLD is, and it was
/// added on measurement rather than principle. Case folding does not
/// strip diacritics, so an accented spelling stays a separate term.
/// Sampling 200,000 real chunks: the corpus writes the same surname both
/// ways, heavily skewed, and neither spelling reaches the other's
/// documents. `Rodriguez` 1,114 occurrences against `Rodríguez` 21,
/// `García` 9 against `Garcia` 1,131, `Núñez` 0 against `Nunez` 116.
/// A litigant's name is the most common thing anyone searches a court
/// corpus for, and today it cannot be searched reliably in either
/// spelling. 1,120 word types are split this way in that sample.
///
/// Two steps the sidecar now offers are deliberately NOT here, on the
/// same evidence. DEHYPHENATE (24) repairs line-break hyphenation, which
/// appears in 0.1% of sampled chunks: this corpus was converted from
/// XML and HTML sources, so end-of-line hyphens were never introduced,
/// and it is PDF-derived text that needs that step. NFKC (12) reaches a
/// similar 0.1%. Neither earns a per-document cost on THIS corpus;
/// both would on a PDF-sourced one, which is why the step is available
/// rather than assumed.
pub fn body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_WHITESPACE,
        stemmer: STEMMER_PORTER,
        term_vector_mode: TERM_VECTOR_MODE_FULL,
        term_vector_source: SOURCE_NORMALIZED_STEMS,
        char_filters: vec![
            CHAR_FILTER_STRIP_INVISIBLE,
            CHAR_FILTER_WHITESPACE,
            CHAR_FILTER_ACCENT_FOLD,
            CHAR_FILTER_FULL_CASE_FOLD,
        ],
    }
}

/// The cased twin of [`body_spec`]: identical tokenizer and stemmer,
/// but term identity taken from the stem of the SURFACE form, so
/// capitalization survives into the term.
///
/// This is not a mistake repeated; it is the other arm of the
/// experiment. Folding is right for recall (`court` should find
/// `Court`), and wrong for the signal that made "Dungeons and Dragons"
/// findable at all: on this corpus the capitalized form dominates for
/// proper nouns, and folding destroys the distinction. Which wins is a
/// question about THIS corpus, so it is measured rather than assumed,
/// which is what a second column is for.
pub fn cased_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        term_vector_source: SOURCE_STEMS,
        // SOURCE_STEMS ignores char filters outright, so leaving them
        // populated here would be a lie about what the column contains.
        char_filters: Vec::new(),
        ..body_spec()
    }
}

/// The analyzers callers may name, for CLIs and config: one vocabulary
/// instead of enum triples at every call site.
///
/// `Ok(None)` means "send no spec", which leaves the sidecar to apply
/// its own defaults. That is a real choice and not the same as any named
/// analyzer, because the sidecar's default does not stem.
pub fn analyzer_by_name(name: &str) -> Result<Option<AnalysisSpec>, String> {
    Ok(match name {
        // `ingest` is the corpus's own analyzer, whatever that currently
        // is; the alias exists so callers can say "match the index"
        // without knowing which one that is today.
        "ingest" | "folded" => Some(body_spec()),
        "cased" => Some(cased_body_spec()),
        "server" => None,
        other => {
            return Err(format!(
                "unknown analyzer {other:?}; expected one of {}",
                ANALYZER_NAMES.join(", ")
            ))
        }
    })
}

/// Every name [`analyzer_by_name`] accepts.
pub const ANALYZER_NAMES: &[&str] = &["ingest", "folded", "cased", "server"];

/// Stable 64-bit identity of an [`AnalysisSpec`]: the term-identity
/// contract reduced to one number a shard can persist and a query can be
/// checked against.
///
/// Field name equality is NOT this check. A column named `body_norm`
/// built under one analyzer and queried under another matches on name,
/// scores different terms, and returns a confident wrong ranking with no
/// error anywhere. That failure has cost this project twice, and the
/// coming rebuild multiplies the surface by adding a second body column
/// whose whole purpose is to differ from the first.
///
/// Hand-rolled FNV-1a for the same reason [`crate::reshard::fnv1a64`] is:
/// this value is written into a FILE FORMAT, so it must never change.
/// `DefaultHasher` is explicitly not stable across Rust releases and
/// would silently invalidate every shard on a toolchain bump.
///
/// `None` is 0, meaning UNKNOWN rather than "the default analyzer": an
/// absent spec is resolved against the sidecar's own defaults, which
/// this side cannot see and must not guess at. 0 never enforces, so a
/// shard written before fingerprints existed keeps answering.
pub fn analysis_fingerprint(spec: Option<&AnalysisSpec>) -> u64 {
    fn eat(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    let Some(spec) = spec else { return 0 };
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    // Domain tag. If AnalysisSpec ever grows a field, bumping this
    // invalidates old fingerprints LOUDLY (every shard mismatches and
    // says so) instead of letting two different specs share a number.
    eat(&mut hash, b"turbovec.analysis.v1");
    for value in [
        spec.tokenizer,
        spec.stemmer,
        spec.term_vector_mode,
        spec.term_vector_source,
    ] {
        eat(&mut hash, &value.to_le_bytes());
    }
    // Char filter ORDER is semantic (it is a chain applied in sequence),
    // so it is hashed in order and never sorted. The count goes in
    // first so [1, 2] and [12] cannot collide.
    eat(&mut hash, &(spec.char_filters.len() as u32).to_le_bytes());
    for filter in &spec.char_filters {
        eat(&mut hash, &filter.to_le_bytes());
    }
    // 0 is reserved for "unknown"; a real spec must never claim it.
    if hash == 0 {
        1
    } else {
        hash
    }
}

/// `AnalysisOptions.Tokenizer.TOKENIZER_WHITESPACE`.
pub const TOKENIZER_WHITESPACE: i32 = 1;
/// `AnalysisOptions.Stemmer.STEMMER_NONE`.
pub const STEMMER_NONE: i32 = 1;
/// `AnalysisOptions.Stemmer.STEMMER_PORTER`.
pub const STEMMER_PORTER: i32 = 2;
/// `TermVectorOptions.Mode.MODE_FULL` (occurrence offsets included).
pub const TERM_VECTOR_MODE_FULL: i32 = 1;
/// `TermVectorOptions.Source.SOURCE_TOKENS` (char filters define identity).
pub const SOURCE_TOKENS: i32 = 1;
/// `TermVectorOptions.Source.SOURCE_STEMS` (char filters IGNORED; see
/// [`body_spec`] for why this is the wrong choice for prose).
pub const SOURCE_STEMS: i32 = 2;
/// `TermVectorOptions.Source.SOURCE_NORMALIZED_STEMS` (char filters, then stem).
pub const SOURCE_NORMALIZED_STEMS: i32 = 3;
/// Strip invisible controls. Sidecar wire value
/// `TermVectorOptions.NormalizerStep.NORMALIZER_STEP_STRIP_INVISIBLE`.
pub const CHAR_FILTER_STRIP_INVISIBLE: i32 = 1;
/// Collapse whitespace. Sidecar wire value
/// `TermVectorOptions.NormalizerStep.NORMALIZER_STEP_WHITESPACE`.
pub const CHAR_FILTER_WHITESPACE: i32 = 2;
/// Full Unicode case fold. Sidecar wire value
/// `TermVectorOptions.NormalizerStep.NORMALIZER_STEP_FULL_CASE_FOLD`.
pub const CHAR_FILTER_FULL_CASE_FOLD: i32 = 6;
/// Unicode NFKC compatibility composition (full-width to ASCII,
/// superscripts to digits, Roman-numeral codepoints to letters). Sidecar
/// wire value `NORMALIZER_STEP_NFKC`. Not in [`body_spec`]: measured at
/// 0.1% of sampled court chunks.
pub const CHAR_FILTER_NFKC: i32 = 12;
/// Strip diacritics, so `Rodríguez` and `Rodriguez` are one term.
/// Sidecar wire value `NORMALIZER_STEP_ACCENT_FOLD`. See [`body_spec`]
/// for the measurement that put it there.
pub const CHAR_FILTER_ACCENT_FOLD: i32 = 15;
/// Rejoin words split by a line-break hyphen. Sidecar wire value
/// `NORMALIZER_STEP_DEHYPHENATE`. Offset-aware, so it is refused under
/// `SOURCE_STEMS`. Not in [`body_spec`]: this corpus came from XML and
/// HTML rather than PDF text, and the pattern appears in 0.1% of
/// sampled chunks. It is the right step for a PDF-sourced corpus.
pub const CHAR_FILTER_DEHYPHENATE: i32 = 24;

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
    let (mode, source, char_filters, tokenizer, stemmer) = match spec {
        Some(s) => (
            s.term_vector_mode,
            s.term_vector_source,
            s.char_filters.clone(),
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
            // The sidecar calls these normalizer STEPS; Lucene (and so
            // this side) calls the stage a char filter. Same field
            // number, same values, different vocabulary.
            steps: char_filters,
            source,
            // One call returning both a folded and an unfolded term
            // stream. Our A/B arms are separate columns with their own
            // specs and fingerprints, so each is requested on its own;
            // this stays off until a caller wants both identities from
            // a single analysis pass.
            dual_cased: false,
        }),
        ..Default::default()
    }
}

/// Zero-length terms dropped at the analysis boundary since process
/// start (see [`analyzed_from`]). The log lines are the visibility;
/// this counter is what keeps them bounded.
static EMPTY_TERMS_DROPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Folds a response's term vectors into a single-field (body)
/// [`AnalyzedDoc`] (term, tf, original-text offsets, and document
/// length).
///
/// Zero-length terms are dropped HERE, at the one place every analysis
/// response passes through. A token made entirely of stripped
/// characters (invisible and format chars: zero-width spaces, soft
/// hyphens, bidi controls, all routine in PDF- and HTML-derived text)
/// normalizes to the empty string under `STRIP_INVISIBLE`, and the
/// sidecar emits that as a term. An empty term is unqueryable by
/// construction, and the index refuses it at open ("directory entry N:
/// empty term") — the wrong moment, hours after a bulk ingest started.
/// Dropping it at the boundary is the same document the sidecar should
/// have produced; the dropped token contributes nothing to the
/// document length either, exactly as if the analyzer had never
/// emitted it.
fn analyzed_from(response: AnalyzeResponse) -> AnalyzedDoc {
    let mut terms = crate::postings::DocTerms::new();
    let mut length = 0u32;
    for tv in response.term_vectors {
        if tv.frequency <= 0 {
            continue;
        }
        if tv.term.is_empty() {
            let n = EMPTY_TERMS_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n <= 20 || n.is_multiple_of(1_000_000) {
                eprintln!(
                    "analysis: dropped zero-length term (occurrence {n} this process): a \
                     token of stripped characters; unqueryable, and refused at index open \
                     if it were kept"
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The two named body analyzers must differ in TERM IDENTITY, not
    /// merely in name. If they ever resolved to the same spec the A/B
    /// they exist for would compare a column with itself and report
    /// "no difference", which is the most expensive possible way to be
    /// wrong: it looks like an answer.
    #[test]
    fn folded_and_cased_are_actually_different_analyzers() {
        let folded = analyzer_by_name("folded").expect("known name").unwrap();
        let cased = analyzer_by_name("cased").expect("known name").unwrap();
        assert_ne!(folded.term_vector_source, cased.term_vector_source);
        assert_eq!(folded.term_vector_source, SOURCE_NORMALIZED_STEMS);
        assert_eq!(cased.term_vector_source, SOURCE_STEMS);
        // Same tokenizer and stemmer: the comparison isolates case
        // folding, so anything else differing would confound it.
        assert_eq!(folded.tokenizer, cased.tokenizer);
        assert_eq!(folded.stemmer, cased.stemmer);
        // SOURCE_STEMS ignores char filters, so carrying them on the
        // cased spec would misdescribe what the column holds.
        assert!(cased.char_filters.is_empty());
        assert!(folded.char_filters.contains(&CHAR_FILTER_FULL_CASE_FOLD));
    }

    /// `ingest` tracks the corpus spec rather than restating it, so the
    /// alias cannot rot when `body_spec` changes.
    #[test]
    fn ingest_is_the_corpus_analyzer() {
        assert_eq!(analyzer_by_name("ingest").unwrap(), Some(body_spec()));
        assert_eq!(analyzer_by_name("folded").unwrap(), Some(body_spec()));
    }

    /// `server` is a real choice (leave the spec unset), not a missing
    /// answer, and it is NOT the same as any named analyzer: the
    /// sidecar's default does not stem.
    #[test]
    fn server_means_no_spec() {
        assert_eq!(analyzer_by_name("server").unwrap(), None);
    }

    /// An unknown name is refused and NAMES the alternatives. A silent
    /// fallback to the default here would query a stemmed index with an
    /// unstemmed spec and report the ranking of whatever fragment
    /// happened to match.
    #[test]
    fn an_unknown_analyzer_is_refused_and_lists_the_known_ones() {
        let err = analyzer_by_name("porter").expect_err("not a known name");
        assert!(err.contains("porter"), "{err}");
        for name in ANALYZER_NAMES {
            assert!(err.contains(name), "error should list {name}: {err}");
        }
    }

    /// The fingerprint must separate the two analyzers an A/B compares,
    /// and must survive a round trip through the file format unchanged.
    #[test]
    fn the_fingerprint_separates_the_analyzers_it_exists_to_separate() {
        let folded = analysis_fingerprint(Some(&body_spec()));
        let cased = analysis_fingerprint(Some(&cased_body_spec()));
        assert_ne!(folded, cased);
        assert_ne!(folded, 0);
        assert_ne!(cased, 0);
        // Stable: same spec, same number, every call.
        assert_eq!(folded, analysis_fingerprint(Some(&body_spec())));
    }

    /// An absent spec is UNKNOWN, not "the default analyzer". The
    /// sidecar resolves an absent spec against its own defaults, which
    /// this side cannot see, so claiming a number here would assert
    /// something we do not know.
    #[test]
    fn an_absent_spec_is_unknown_not_a_default() {
        assert_eq!(analysis_fingerprint(None), 0);
    }

    /// Char filter ORDER is semantic, and the count is hashed so a
    /// two-filter chain cannot collide with a one-filter chain whose
    /// value happens to concatenate.
    #[test]
    fn char_filter_order_and_arity_both_change_the_fingerprint() {
        let base = body_spec();
        let mut reordered = base.clone();
        reordered.char_filters.reverse();
        assert_ne!(
            analysis_fingerprint(Some(&base)),
            analysis_fingerprint(Some(&reordered)),
            "a reordered filter chain is a different analyzer"
        );
        let mut dropped = base.clone();
        dropped.char_filters.pop();
        assert_ne!(
            analysis_fingerprint(Some(&base)),
            analysis_fingerprint(Some(&dropped))
        );
        let mut one = base.clone();
        one.char_filters = vec![12];
        let mut two = base.clone();
        two.char_filters = vec![1, 2];
        assert_ne!(
            analysis_fingerprint(Some(&one)),
            analysis_fingerprint(Some(&two)),
            "[12] must not collide with [1, 2]"
        );
    }

    /// Every scalar of the spec participates. A field that did not would
    /// be a hole in the contract exactly where someone would eventually
    /// vary it.
    #[test]
    fn every_field_of_the_spec_changes_the_fingerprint() {
        let base = body_spec();
        let fp = analysis_fingerprint(Some(&base));
        for (name, mutate) in [
            (
                "tokenizer",
                (|s: &mut AnalysisSpec| s.tokenizer += 1) as fn(&mut AnalysisSpec),
            ),
            ("stemmer", |s: &mut AnalysisSpec| s.stemmer += 1),
            ("term_vector_mode", |s: &mut AnalysisSpec| {
                s.term_vector_mode += 1
            }),
            ("term_vector_source", |s: &mut AnalysisSpec| {
                s.term_vector_source += 1
            }),
        ] {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(
                fp,
                analysis_fingerprint(Some(&changed)),
                "changing {name} must change the fingerprint"
            );
        }
    }

    /// The value is written into a FILE FORMAT, so it is pinned to a
    /// literal. A change here silently invalidates every shard on disk,
    /// so it must be a deliberate edit to this test and not a drive-by
    /// refactor of the hash.
    #[test]
    fn the_corpus_fingerprint_is_pinned() {
        assert_eq!(
            analysis_fingerprint(Some(&body_spec())),
            0x55eb_d3a6_febd_2ac3,
            "the corpus analyzer fingerprint changed; any shard built under \
             the old value will refuse every query against this spec, so \
             changing it commits to a rebuild"
        );
    }

    /// The steps measurement said to skip are skipped, and the one it
    /// said to adopt is adopted. Pinned because the argument for each is
    /// a corpus measurement recorded in `body_spec`'s docs, not a
    /// preference: a later edit that quietly adds DEHYPHENATE or NFKC to
    /// the corpus analyzer should have to change this test and say why.
    #[test]
    fn the_corpus_analyzer_folds_accents_and_skips_the_pdf_steps() {
        let filters = body_spec().char_filters;
        assert!(
            filters.contains(&CHAR_FILTER_ACCENT_FOLD),
            "accent folding is what makes Rodriguez and Rodríguez one term"
        );
        assert!(
            !filters.contains(&CHAR_FILTER_DEHYPHENATE),
            "this corpus is XML/HTML-derived; line-break hyphens are 0.1% of chunks"
        );
        assert!(!filters.contains(&CHAR_FILTER_NFKC));
        // Folding must not disturb the cased arm: it takes identity from
        // the surface stem and ignores char filters outright.
        assert!(cased_body_spec().char_filters.is_empty());
        assert_ne!(
            analysis_fingerprint(Some(&body_spec())),
            analysis_fingerprint(Some(&cased_body_spec())),
            "the two A/B arms must stay distinguishable on the wire"
        );
    }

    /// A token made entirely of stripped characters normalizes to the
    /// empty string, which the sidecar emits as a term; the boundary
    /// drops it, and its tf never reaches the document length. The
    /// result is exactly the document the analyzer should have
    /// produced.
    #[test]
    fn zero_length_terms_are_dropped_at_the_boundary() {
        use crate::pb::analysis::{Span, TermVector};
        let tv = |term: &str, tf: i32| TermVector {
            term: term.to_string(),
            frequency: tf,
            occurrences: vec![Span {
                start: 0,
                end: 5,
                ..Default::default()
            }],
            ..Default::default()
        };
        let with_empty = AnalyzeResponse {
            term_vectors: vec![tv("court", 2), tv("", 3), tv("appeal", 1)],
            ..Default::default()
        };
        let without = AnalyzeResponse {
            term_vectors: vec![tv("court", 2), tv("appeal", 1)],
            ..Default::default()
        };
        assert_eq!(
            analyzed_from(with_empty),
            analyzed_from(without),
            "the empty term must vanish as if the analyzer never emitted it"
        );
    }
}
