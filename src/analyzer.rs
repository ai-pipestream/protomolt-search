//! Lexical analysis providers and the OpenNLP sidecar client (vendored proto
//! `ai.pipestream.opennlp.analysis.v1`).
//!
//! `native` runs the same product term contract in-process through the
//! cross-platform `protomolt-analyzer` crate. HTTP(S) values use the OpenNLP
//! sidecar. Search configures both to produce original-text UTF-16 offsets;
//! direct users of either analyzer can select UTF-8 instead. Embeddings and optional
//! model-backed/structural layers remain sidecar capabilities.

#[cfg(feature = "net")]
use std::collections::HashMap;
#[cfg(feature = "net")]
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[cfg(feature = "net")]
use tokio_stream::wrappers::ReceiverStream;
#[cfg(feature = "net")]
use tonic::transport::Channel;
use tonic::{Status, Streaming};

#[cfg(feature = "net")]
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

/// Configuration value selecting the in-process Rust analyzer.
pub const NATIVE_ANALYSIS_BACKEND: &str = "native";

/// Resolved lexical analysis provider. The string configuration stays
/// backward-compatible with sidecar addresses while callers that want a
/// typed boundary can parse it once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisBackend {
    Native,
    Sidecar(String),
}

impl AnalysisBackend {
    pub fn parse(value: &str) -> Result<Self, Status> {
        match value.trim() {
            "native" | "native://" => Ok(Self::Native),
            "" => Err(Status::invalid_argument("analysis backend is empty")),
            address => Ok(Self::Sidecar(address.to_string())),
        }
    }
}

/// The analysis a body-text corpus is built with, and the ONLY spec that
/// may be used to query one.
///
/// Term identity is decided by the configured analysis provider, so an index
/// and a query that disagree about this struct do not fail — they silently
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

/// Unicode word-boundary variant of [`body_spec`]. This is additive rather
/// than the default because changing the tokenizer changes persisted term
/// identity and requires a new index generation.
pub fn uax29_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_UAX29,
        ..body_spec()
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
/// `AnalysisOptions.Tokenizer.TOKENIZER_UAX29`.
pub const TOKENIZER_UAX29: i32 = 3;
/// `AnalysisOptions.Stemmer.STEMMER_NONE`.
pub const STEMMER_NONE: i32 = 1;
/// `AnalysisOptions.Stemmer.STEMMER_PORTER`.
pub const STEMMER_PORTER: i32 = 2;
/// `TermVectorOptions.Mode.MODE_FULL` (occurrence offsets included).
pub const TERM_VECTOR_MODE_FULL: i32 = 1;
/// `TermVectorOptions.Mode.MODE_SCORING_ONLY` (frequency only).
pub const TERM_VECTOR_MODE_SCORING_ONLY: i32 = 2;
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
/// Simple (per-character) case folding; the sidecar's other case step.
/// A cased twin drops it along with the full fold.
pub const CHAR_FILTER_CASE_FOLD: i32 = 16;
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
/// Bring a term prefix into a field's term identity for dictionary
/// expansion (`docs/prefix-terms.md`): the spec's normalizer chain, and
/// never its stemmer. Under `SOURCE_STEMS` the chain is ignored at ingest,
/// so the prefix is compared as written. An absent spec cannot be
/// honored — the sidecar's default chain is not known here, and a
/// prefix normalized under the wrong chain matches the wrong terms — so
/// it refuses by name, as does a chain the native normalizer does not
/// implement.
pub fn normalize_prefix(prefix: &str, spec: Option<&AnalysisSpec>) -> Result<String, Status> {
    if prefix.is_empty() {
        return Err(Status::invalid_argument(
            "a term prefix must be non-empty; an empty prefix is every term",
        ));
    }
    let Some(spec) = spec else {
        return Err(Status::invalid_argument(
            "term prefixes need an explicit AnalysisSpec: the prefix is normalized under \
             the field's char filters before it is compared against the dictionary, and \
             the sidecar's default chain is not known to the coordinator",
        ));
    };
    if spec.term_vector_source == SOURCE_STEMS {
        return Ok(prefix.to_string());
    }
    let native = native_spec(Some(spec))?;
    let normalized = protomolt_analyzer::normalize_term(prefix, &native.normalizers);
    if normalized.is_empty() {
        return Err(Status::invalid_argument(format!(
            "term prefix {prefix:?} normalizes to nothing under the field's char filters"
        )));
    }
    Ok(normalized)
}

#[cfg(feature = "net")]
type AnalysisChannels = Mutex<HashMap<String, Channel>>;
#[cfg(feature = "net")]
type RuntimeChannelPools = Mutex<HashMap<tokio::runtime::Id, Weak<AnalysisChannels>>>;
#[cfg(feature = "net")]
static CHANNEL_POOLS: OnceLock<RuntimeChannelPools> = OnceLock::new();

/// A tonic channel's worker belongs to the runtime that created it. A weak
/// registry lets callers share one pool per runtime without keeping retired
/// pools or their channels alive for the rest of the process.
#[cfg(feature = "net")]
fn runtime_channel_pool() -> Result<Arc<AnalysisChannels>, Status> {
    let handle = tokio::runtime::Handle::try_current().map_err(|_| {
        Status::failed_precondition("analysis channels require an active Tokio runtime")
    })?;
    let registry = CHANNEL_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut pools = registry.lock().expect("channel pool registry poisoned");
    pools.retain(|_, pool| pool.strong_count() != 0);
    if let Some(pool) = pools.get(&handle.id()).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let pool = Arc::new(Mutex::new(HashMap::new()));
    pools.insert(handle.id(), Arc::downgrade(&pool));
    // The runtime owns this strong reference, including when it shuts down
    // before ever polling the task. Dropping the task releases the pool.
    let owner = Arc::clone(&pool);
    handle.spawn(async move {
        std::future::pending::<()>().await;
        drop(owner);
    });
    Ok(pool)
}

/// Reuse an analysis channel within the current Tokio runtime. Call again
/// after replacing that runtime; an existing Channel cannot outlive its worker.
#[cfg(feature = "net")]
pub fn shared_channel(addr: &str) -> Result<Channel, Status> {
    let pool = runtime_channel_pool()?;
    let mut channels = pool.lock().expect("analysis channel pool poisoned");
    if let Some(channel) = channels.get(addr) {
        return Ok(channel.clone());
    }
    // Creation is synchronous and remains under the pool lock so concurrent
    // callers cannot open competing channels for the same address.
    let channel = Channel::from_shared(addr.to_string())
        .map_err(|error| {
            Status::invalid_argument(format!("bad sidecar address {addr:?}: {error}"))
        })?
        .connect_lazy();
    channels.insert(addr.to_string(), channel.clone());
    Ok(channel)
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
    if AnalysisBackend::parse(addr)? == AnalysisBackend::Native {
        return analyze_document_native(text, spec);
    }
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
        options: Some(analysis_options(spec, SessionLayers::default())),
    };
    #[cfg(feature = "net")]
    {
        let mut client = client(addr)?;
        // Raw Status passthrough: transport failures keep tonic's
        // Unavailable (the channel connects lazily, so "sidecar down"
        // surfaces HERE, not at client construction), server errors keep
        // their own codes.
        let response = client.analyze(request).await?.into_inner();
        analyzed_from(response, SessionLayers::default())
    }
    #[cfg(not(feature = "net"))]
    {
        let _ = request;
        Err(sidecar_needs_net(addr))
    }
}

/// Analyze one document with the in-process Rust provider.
///
/// Native analysis requires an explicit spec. The sidecar-only `server`
/// analyzer has no stable client-visible default to reproduce, so guessing it
/// here would make the same fingerprint mean different terms.
pub fn analyze_document_native(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    Ok(native_analysis(text, spec)?.doc)
}

/// Both identities from one native pass (`docs/dual-cased.md`): the
/// folded body at field 0 and the cased twin in `cased`.
pub fn analyze_document_native_dual(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    validate_dual_cased_spec(spec)?;
    let spec = native_spec(spec)?;
    Ok(native_dual_with_spec(text, &spec)?.doc)
}

#[derive(Debug)]
struct NativeAnalysis {
    doc: AnalyzedDoc,
    tokens: Vec<String>,
}

fn native_analysis(text: &str, spec: Option<&AnalysisSpec>) -> Result<NativeAnalysis, Status> {
    let spec = native_spec(spec)?;
    native_analysis_with_spec(text, &spec)
}

fn native_analysis_with_spec(
    text: &str,
    spec: &protomolt_analyzer::AnalysisSpec,
) -> Result<NativeAnalysis, Status> {
    let analyzed = protomolt_analyzer::analyze(text, spec)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let tokens = analyzed.tokens.clone();
    Ok(NativeAnalysis {
        doc: AnalyzedDoc {
            fields: vec![native_field(analyzed)],
            cased: None,
            quality: None,
            geography: None,
            entities: Vec::new(),
        },
        tokens,
    })
}

/// Both identities from one native pass (`docs/dual-cased.md`).
fn native_dual_with_spec(
    text: &str,
    spec: &protomolt_analyzer::AnalysisSpec,
) -> Result<NativeAnalysis, Status> {
    let dual = protomolt_analyzer::analyze_dual(text, spec)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let tokens = dual.folded.tokens.clone();
    Ok(NativeAnalysis {
        doc: AnalyzedDoc {
            fields: vec![native_field(dual.folded)],
            cased: Some(native_field(dual.cased)),
            quality: None,
            geography: None,
            entities: Vec::new(),
        },
        tokens,
    })
}

/// One native identity stream as an analyzed field. The native
/// tokenizer numbers its own tokens, so positions come out of the same
/// pass as the terms (no second walk), and its newline sentence detector
/// runs in that pass too, so every native analysis carries a sentence
/// table (docs/highlighting.md).
fn native_field(analyzed: protomolt_analyzer::AnalyzedDocument) -> crate::postings::AnalyzedField {
    let mut terms = crate::postings::DocTerms::with_capacity(analyzed.term_vectors.len());
    let mut positions = crate::postings::DocPositions::with_capacity(analyzed.term_vectors.len());
    for vector in analyzed.term_vectors {
        terms.push((
            vector.term,
            vector.frequency,
            vector
                .occurrences
                .into_iter()
                .map(|span| (span.start, span.end))
                .collect(),
        ));
        positions.push(vector.positions);
    }
    crate::postings::AnalyzedField {
        terms,
        length: analyzed.length,
        positions: Some(positions),
        sentences: Some(
            analyzed
                .sentences
                .iter()
                .map(|span| (span.start, span.end))
                .collect(),
        ),
    }
}

fn native_spec(spec: Option<&AnalysisSpec>) -> Result<protomolt_analyzer::AnalysisSpec, Status> {
    use protomolt_analyzer::{
        AnalysisSpec as NativeSpec, NormalizerStep, Stemmer, TermVectorMode, TermVectorSource,
        Tokenizer,
    };

    let spec = spec.ok_or_else(|| {
        Status::failed_precondition(
            "native analysis requires an explicit AnalysisSpec; analyzer 'server' is sidecar-only",
        )
    })?;
    let tokenizer = match spec.tokenizer {
        0 | TOKENIZER_WHITESPACE => Tokenizer::Whitespace,
        TOKENIZER_UAX29 => Tokenizer::Uax29,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports TOKENIZER_WHITESPACE (1) and TOKENIZER_UAX29 (3), got {value}"
            )))
        }
    };
    let stemmer = match spec.stemmer {
        0 | STEMMER_NONE => Stemmer::None,
        STEMMER_PORTER => Stemmer::Porter,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports only STEMMER_NONE (1) and STEMMER_PORTER (2), got {value}"
            )))
        }
    };
    let term_vector_mode = match spec.term_vector_mode {
        0 | TERM_VECTOR_MODE_FULL => TermVectorMode::Full,
        TERM_VECTOR_MODE_SCORING_ONLY => TermVectorMode::ScoringOnly,
        value => {
            return Err(Status::invalid_argument(format!(
            "native analysis supports term vector modes FULL (1) and SCORING_ONLY (2), got {value}"
        )))
        }
    };
    let term_vector_source = match spec.term_vector_source {
        0 | SOURCE_TOKENS => TermVectorSource::Tokens,
        SOURCE_STEMS => TermVectorSource::Stems,
        SOURCE_NORMALIZED_STEMS => TermVectorSource::NormalizedStems,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports term vector sources TOKENS (1), STEMS (2), and NORMALIZED_STEMS (3), got {value}"
            )))
        }
    };
    let mut normalizers = Vec::with_capacity(spec.char_filters.len());
    for &value in &spec.char_filters {
        let step = match value {
            0 => continue,
            CHAR_FILTER_STRIP_INVISIBLE => NormalizerStep::StripInvisible,
            CHAR_FILTER_WHITESPACE => NormalizerStep::Whitespace,
            CHAR_FILTER_ACCENT_FOLD => NormalizerStep::AccentFold,
            CHAR_FILTER_FULL_CASE_FOLD => NormalizerStep::FullCaseFold,
            unsupported => {
                return Err(Status::invalid_argument(format!(
                    "native analysis does not implement normalizer step {unsupported}; supported values are STRIP_INVISIBLE (1), WHITESPACE (2), FULL_CASE_FOLD (6), and ACCENT_FOLD (15)"
                )))
            }
        };
        normalizers.push(step);
    }
    let native = NativeSpec {
        tokenizer,
        stemmer,
        term_vector_mode,
        term_vector_source,
        normalizers,
    };
    if matches!(
        native.term_vector_source,
        TermVectorSource::Stems | TermVectorSource::NormalizedStems
    ) && native.stemmer == Stemmer::None
    {
        return Err(Status::invalid_argument(
            "term vector source STEMS requires a stemmer other than STEMMER_NONE",
        ));
    }
    Ok(native)
}

/// The refusal a sidecar address gets from a build without the network
/// stack: the embedded runtime analyzes natively and dials nothing.
#[cfg(not(feature = "net"))]
fn sidecar_needs_net(addr: &str) -> Status {
    Status::failed_precondition(format!(
        "analysis sidecar {addr:?} is unreachable from this build: the `net` feature is off, so \
         nothing dials; use --analysis-addr=native"
    ))
}

#[cfg(feature = "net")]
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
    if AnalysisBackend::parse(addr)? == AnalysisBackend::Native {
        return Err(Status::failed_precondition(
            "the native lexical analyzer does not provide embeddings; configure an OpenNLP sidecar or another embedding provider",
        ));
    }
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
    #[cfg(not(feature = "net"))]
    {
        let _ = request;
        Err(sidecar_needs_net(addr))
    }
    #[cfg(feature = "net")]
    {
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
}

/// Which optional sidecar layers an analysis SESSION requests beyond
/// term identity. A property of the session, not of a document: the
/// layers ride the options message a stream opens with, so a change
/// reopens the session exactly as a spec change does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionLayers {
    /// The noise and artifact scanners (`docs/quality-columns.md`).
    pub quality: bool,
    /// The geocoding layer (`docs/geography-columns.md`). Requires the
    /// sidecar to serve NER; opening preflights that capability.
    pub geography: bool,
    /// Named entity mentions for materialization into a product-owned map
    /// column. Requires a configured sidecar NER model.
    pub entities: bool,
    /// The sentence layer (`docs/highlighting.md`): spans in original-text
    /// coordinates, stored per document for sentence-bounded snippets.
    /// Model-free by default on the sidecar (its newline detector) and
    /// one traversal of text it already holds.
    pub sentences: bool,
    /// Both term identities from one pass (`docs/dual-cased.md`): the
    /// sidecar's `dual_cased`, or the native analyzer's twin accumulator.
    /// Opening a sidecar session preflights
    /// `GetCapabilities.dual_term_identity_available`.
    pub dual_cased: bool,
}

/// Maps `spec` straight onto the sidecar's `AnalysisOptions`: term vectors
/// are always requested (FULL mode with occurrence offsets unless the spec
/// overrides), everything else defaults.
fn analysis_options(spec: Option<&AnalysisSpec>, layers: SessionLayers) -> AnalysisOptions {
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
            // One call returning both the folded and the cased term
            // stream (`docs/dual-cased.md`): on when the ingest names a
            // cased field, so the A/B pair costs one pass.
            dual_cased: layers.dual_cased,
        }),
        // The quality layers ride the SAME analysis pass as the terms
        // (docs/quality-columns.md). Both are model-free structural
        // scanners, so asking for them costs one traversal of text the
        // sidecar is already holding — and the query path gains nothing
        // to do, because what comes back becomes an ordinary column.
        noise: layers.quality,
        artifacts: layers.quality,
        // Geocoding consumes the entity layer; explicit entity columns do as
        // well. Availability was preflighted at session open.
        ner: layers.entities || layers.geography,
        geo: layers.geography,
        // Sentence spans for snippets (docs/highlighting.md) ride the
        // same pass; the term identity is unchanged by the layer.
        sentence_detection: layers.sentences,
        // Search persistence remains one unambiguous legacy coordinate system.
        // Offset output selection is separate from term identity and is exposed
        // by the portable analyzer API rather than AnalysisSpec.
        offset_unit: crate::pb::analysis::OffsetUnit::Utf16CodeUnits as i32,
        ..Default::default()
    }
}

/// Zero-length terms dropped at the analysis boundary since process
/// start (see [`analyzed_from`]). The log lines are the visibility;
/// this counter is what keeps them bounded.
static EMPTY_TERMS_DROPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The cased twin of an analysis spec (`docs/dual-cased.md`): the same
/// tokenizer, stemmer, mode, and source, and the same step chain minus
/// case folding — what the sidecar's `cased_term_vectors` and the native
/// analyzer's cased accumulator compute. Its fingerprint is the cased
/// field's fingerprint.
pub fn cased_twin_spec(spec: &AnalysisSpec) -> AnalysisSpec {
    AnalysisSpec {
        char_filters: spec
            .char_filters
            .iter()
            .copied()
            .filter(|step| *step != CHAR_FILTER_FULL_CASE_FOLD && *step != CHAR_FILTER_CASE_FOLD)
            .collect(),
        ..spec.clone()
    }
}

/// The body spec a cased field can be derived from: explicit, and with
/// a step-chain source (`SOURCE_STEMS` ignores the chain, so it has no
/// folded form to contrast with).
pub fn validate_dual_cased_spec(spec: Option<&AnalysisSpec>) -> Result<(), Status> {
    let Some(spec) = spec else {
        return Err(Status::invalid_argument(
            "cased_field needs an explicit body AnalysisSpec: the cased field's fingerprint is \
             the twin of the body's, and analyzer 'server' describes none",
        ));
    };
    if spec.term_vector_source == SOURCE_STEMS {
        return Err(Status::invalid_argument(
            "cased_field needs a step-chain term vector source on the body (SOURCE_TOKENS or \
             SOURCE_NORMALIZED_STEMS); SOURCE_STEMS ignores the step chain, so it has no folded \
             form to contrast with",
        ));
    }
    Ok(())
}

/// Term vectors to per-document terms: `(terms, length)`, dropping
/// zero-frequency and zero-length identities.
fn terms_of(vectors: Vec<crate::pb::analysis::TermVector>) -> (crate::postings::DocTerms, u32) {
    let mut terms = crate::postings::DocTerms::new();
    let mut length = 0u32;
    for tv in vectors {
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
    (terms, length)
}

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
fn analyzed_from(
    mut response: AnalyzeResponse,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    match crate::pb::analysis::OffsetUnit::try_from(response.offset_unit) {
        Ok(crate::pb::analysis::OffsetUnit::Unspecified)
        | Ok(crate::pb::analysis::OffsetUnit::Utf16CodeUnits) => {}
        Ok(crate::pb::analysis::OffsetUnit::Utf8Bytes) => {
            return Err(Status::failed_precondition(
                "analysis sidecar returned UTF-8 byte offsets after search requested UTF-16; refusing ambiguous persisted spans",
            ));
        }
        Err(value) => {
            return Err(Status::failed_precondition(format!(
                "analysis sidecar returned unknown offset unit {value}"
            )));
        }
    }
    let quality = layers.quality.then(|| doc_quality(&response));
    let geography = layers.geography.then(|| doc_geography(&response));
    let entities = if layers.entities {
        response
            .entities
            .iter()
            .map(|entity| {
                let span = entity.span.as_ref();
                crate::phrases::DocEntity {
                    kind: entity.r#type.clone(),
                    text: entity.text.clone(),
                    start: span.map_or(0, |span| span.start.max(0) as u32),
                    end: span.map_or(0, |span| span.end.max(0) as u32),
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let (terms, length) = terms_of(std::mem::take(&mut response.term_vectors));
    let positions = token_positions(&response.tokens, &terms)?;
    // The sentence layer is the answer only when it was asked for: a
    // response without it for a session that requested it is a table
    // of zero sentences, which the sentence field's coverage check
    // refuses for any document with a term (docs/highlighting.md).
    let sentences = layers.sentences.then(|| {
        response
            .sentences
            .iter()
            .map(|s| (s.start.max(0) as u32, s.end.max(0) as u32))
            .collect::<Vec<(u32, u32)>>()
    });
    let mut doc = AnalyzedDoc::body(terms, length);
    doc.fields[0].positions = positions;
    doc.fields[0].sentences = sentences.clone();
    if layers.dual_cased {
        // The cased identity of the same pass: the same tokens, so the
        // same token layer derives its ordinals and the same sentences
        // bound it.
        let (cased_terms, cased_length) =
            terms_of(std::mem::take(&mut response.cased_term_vectors));
        if cased_terms.is_empty() && !doc.fields[0].terms.is_empty() {
            return Err(Status::failed_precondition(
                "dual_cased was requested but the analysis sidecar returned no cased term \
                 identities; the running jar ignores the flag (an open port is not the jar); \
                 rebuild grpc-opennlp-analysis from main",
            ));
        }
        let cased_positions = token_positions(&response.tokens, &cased_terms)?;
        doc.cased = Some(crate::postings::AnalyzedField {
            terms: cased_terms,
            length: cased_length,
            positions: cased_positions,
            sentences,
        });
    }
    doc.quality = quality;
    doc.geography = geography;
    doc.entities = entities;
    Ok(doc)
}

/// Token ordinals for every occurrence in `terms`, read off the
/// response's token layer (`docs/phrase-proximity.md`). This is the ONE
/// sidecar call ingest already makes: the token layer rides the same
/// response as the term vectors, so positions cost no round trip.
///
/// The sidecar's contract makes each occurrence span a token span, and
/// the ordinal of a token is its index in document order. The match is
/// a single merge over both lists sorted by start offset (no hashing,
/// no per-occurrence scan). A response without a token layer yields
/// `None`; a positional field then refuses the document by name rather
/// than guessing adjacency from the spans. An occurrence that is not a
/// token span is a contract break and refuses outright.
fn token_positions(
    tokens: &[crate::pb::analysis::Token],
    terms: &crate::postings::DocTerms,
) -> Result<Option<crate::postings::DocPositions>, Status> {
    let occurrences: usize = terms.iter().map(|(_, _, offsets)| offsets.len()).sum();
    if occurrences == 0 {
        // Nothing to place: scoring-only vectors, or an empty document.
        return Ok(Some(terms.iter().map(|_| Vec::new()).collect()));
    }
    if tokens.is_empty() {
        return Ok(None);
    }
    // (start, end, term index, occurrence index), sorted by start; ties
    // cannot happen for distinct tokens, and two terms cannot share one.
    let mut wanted: Vec<(u32, u32, usize, usize)> = Vec::with_capacity(occurrences);
    for (ti, (_, _, offsets)) in terms.iter().enumerate() {
        for (oi, &(start, end)) in offsets.iter().enumerate() {
            wanted.push((start, end, ti, oi));
        }
    }
    wanted.sort_unstable();
    let mut positions: crate::postings::DocPositions = terms
        .iter()
        .map(|(_, _, offsets)| vec![0u32; offsets.len()])
        .collect();
    let mut cursor = 0usize;
    for (start, end, ti, oi) in wanted {
        while cursor < tokens.len() {
            let span = tokens[cursor].span.as_ref();
            let token_start = span.map_or(0, |s| s.start.max(0) as u32);
            if token_start >= start {
                break;
            }
            cursor += 1;
        }
        let matched = tokens
            .get(cursor)
            .and_then(|t| t.span.as_ref())
            .is_some_and(|s| s.start.max(0) as u32 == start && s.end.max(0) as u32 == end);
        if !matched {
            return Err(Status::failed_precondition(format!(
                "analysis sidecar reported occurrence [{start}, {end}) of {:?} that is not a \
                 token span; token positions cannot be derived from this response",
                terms[ti].0
            )));
        }
        positions[ti][oi] = u32::try_from(cursor).expect("token count fits u32 under the text cap");
        cursor += 1;
    }
    Ok(Some(positions))
}

/// Fold a response's noise and artifact layers into the per-document
/// scalars the quality columns hold (`docs/quality-columns.md`).
///
/// `noise` is the worst finding's score, which the sidecar defines in
/// (0, 1]; a document with no findings scores exactly 0. `noise_chars`
/// is the length of the UNION of the findings' spans, computed by a
/// sweep over the spans sorted by start, so overlapping findings are
/// counted once and the answer does not depend on the order the
/// sidecar happened to emit them in. `artifacts` is a count, which is
/// exact by construction.
fn doc_quality(response: &AnalyzeResponse) -> crate::postings::DocQuality {
    let mut worst = 0.0f64;
    let mut spans: Vec<(i64, i64)> = Vec::with_capacity(response.noise.len());
    for finding in &response.noise {
        // A non-finite score would poison the f64 column's min/max
        // metadata, which score-chain bounds read. Treat it as no
        // signal rather than propagating a NaN into the ranking.
        if finding.score.is_finite() && finding.score > worst {
            worst = finding.score;
        }
        if let Some(span) = finding.span.as_ref() {
            let (start, end) = (i64::from(span.start), i64::from(span.end));
            if end > start {
                spans.push((start, end));
            }
        }
    }
    spans.sort_unstable();
    let mut covered = 0i64;
    let mut reach = i64::MIN;
    for (start, end) in spans {
        let start = start.max(reach);
        if end > start {
            covered += end - start;
        }
        reach = reach.max(end);
    }
    crate::postings::DocQuality {
        noise: worst,
        noise_chars: covered,
        artifacts: response.artifacts.len() as i64,
    }
}

/// Reduce a response's geocoding layer to the per-document scalars the
/// geography columns hold (`docs/geography-columns.md`).
///
/// The point is the highest-confidence location, ties broken by text
/// order (the sidecar emits locations in text order, and a stable
/// `>` scan keeps the first). A non-finite confidence is no signal and
/// cannot be chosen. The country is the top region vote's — the
/// sidecar ranks votes by share — independent of which location won,
/// because the vote aggregates ALL the document's evidence while the
/// point is one best mention. A document with no locations reduces to
/// an all-absent value, which materializes as no columns at all.
fn doc_geography(response: &AnalyzeResponse) -> crate::postings::DocGeography {
    let mut best: Option<&crate::pb::analysis::GeoLocation> = None;
    for location in &response.locations {
        if !location.confidence.is_finite() {
            continue;
        }
        if best.is_none_or(|b| location.confidence > b.confidence) {
            best = Some(location);
        }
    }
    crate::postings::DocGeography {
        point: best.map(|b| (b.latitude, b.longitude)),
        confidence: best.map_or(0.0, |b| b.confidence),
        country: response
            .regions
            .first()
            .map(|r| r.country_code.clone())
            .unwrap_or_default(),
    }
}

/// Client-side submission buffer of an [`AnalyzeStream`]. Pacing is the
/// server's job (it grants transport credit from its worker capacity);
/// this only bounds local queuing before `submit` awaits.
const SUBMIT_BUFFER: usize = 32;

/// A cloneable submission handle for an open [`AnalyzeStream`].
#[derive(Clone)]
pub struct AnalyzeSubmit {
    requests: AnalyzeRequests,
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
#[derive(Clone)]
enum AnalyzeRequests {
    Sidecar(tokio::sync::mpsc::Sender<AnalyzeStreamRequest>),
    Native(tokio::sync::mpsc::Sender<NativeRequest>),
}

struct NativeRequest {
    sequence: u64,
    text: String,
}

struct NativeResponse {
    sequence: u64,
    result: Result<NativeAnalysis, Status>,
}

impl AnalyzeSubmit {
    /// Queue one document, tagged with a caller-chosen sequence that the
    /// matching result echoes. Awaits only when the local buffer is full,
    /// which means the server has not granted credit yet: the await IS
    /// the backpressure. UNAVAILABLE when the stream is gone.
    pub async fn submit(&self, sequence: u64, text: &str) -> Result<(), Status> {
        match &self.requests {
            AnalyzeRequests::Sidecar(requests) => requests
                .send(AnalyzeStreamRequest {
                    msg: Some(analyze_stream_request::Msg::Doc(AnalyzeStreamDoc {
                        sequence,
                        text: text.to_string(),
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("analysis stream closed")),
            AnalyzeRequests::Native(requests) => requests
                .send(NativeRequest {
                    sequence,
                    text: text.to_string(),
                })
                .await
                .map_err(|_| Status::unavailable("native analysis stream closed")),
        }
    }
}

/// One AnalyzeStream call: many documents over one bidi RPC for one
/// analysis spec, paced end to end by the sidecar's server-side flow
/// control. Results arrive in COMPLETION order, tagged with the
/// submitted sequence; callers that need arrival order reorder.
pub struct AnalyzeStream {
    submit: Option<AnalyzeSubmit>,
    responses: AnalyzeResponses,
    /// The shard's vocabulary listener, when vocabulary accumulation is
    /// enabled (`None` costs one branch per response). Feeding happens
    /// HERE — the only layer where the raw response's `tokens` (surface
    /// forms, dropped by `analyzed_from`) and `term_vectors` both still
    /// exist — and only on this bulk path: unary `Analyze` is the query
    /// path, and query text never enters corpus statistics.
    vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
    /// Which optional layers this session asked for, which decides
    /// whether a response's empty `noise` or `locations` list means
    /// "measured, found nothing" or "not requested". Fixed for the
    /// session's lifetime, like the options message that set it.
    layers: SessionLayers,
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
enum AnalyzeResponses {
    Sidecar(Box<Streaming<AnalyzeStreamResponse>>),
    Native(tokio::sync::mpsc::Receiver<NativeResponse>),
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
        Self::open_with_vocab(addr, spec, None, SessionLayers::default()).await
    }

    /// [`open`](Self::open) with the shard's vocabulary listener attached:
    /// every successfully analyzed response feeds its term vectors (TERMS
    /// channel) and raw token texts (TOKENS channel) before the response
    /// is folded into an [`AnalyzedDoc`].
    /// `layers` asks the sidecar for its optional layers on the same
    /// pass (`docs/quality-columns.md`, `docs/geography-columns.md`).
    /// They are a property of the SESSION, not of a document, because
    /// they are requested in the options message a stream opens with —
    /// which is why a mid-stream change reopens the session, exactly
    /// as a spec change does.
    ///
    /// A session asking for geography preflights the sidecar's NER
    /// capability and REFUSES when it has none: the sidecar's own
    /// behavior on that state — empty layers plus a free-form warning
    /// per response — is indistinguishable from "no locations found",
    /// and would silently ingest an entire corpus as place-less.
    pub async fn open_with_vocab(
        addr: &str,
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        if AnalysisBackend::parse(addr)? == AnalysisBackend::Native {
            return Self::open_native(spec, vocab, layers);
        }
        #[cfg(not(feature = "net"))]
        {
            let _ = (spec, vocab, layers);
            Err(sidecar_needs_net(addr))
        }
        #[cfg(feature = "net")]
        {
            let mut client = client(addr)?;
            if layers.dual_cased {
                validate_dual_cased_spec(spec)?;
            }
            if layers.geography || layers.entities || layers.dual_cased {
                let capabilities = client
                    .get_capabilities(crate::pb::analysis::GetCapabilitiesRequest {})
                    .await?
                    .into_inner();
                if (layers.geography || layers.entities) && !capabilities.ner_available {
                    return Err(Status::failed_precondition(
                        "entity or geography columns were requested but this sidecar has no NER model configured (GetCapabilities.ner_available = false); configure an NER model or disable those columns",
                    ));
                }
                // An open port is not the jar: the running sidecar must say
                // it serves both identities before ingest depends on it.
                if layers.dual_cased && !capabilities.dual_term_identity_available {
                    return Err(Status::failed_precondition(format!(
                        "a cased field was requested but the sidecar at {addr} does not serve the \
                         dual term identity (GetCapabilities.dual_term_identity_available = false); \
                         rebuild grpc-opennlp-analysis from main (./gradlew installDist) or drop \
                         cased_field"
                    )));
                }
            }
            let (requests, feed) = tokio::sync::mpsc::channel(SUBMIT_BUFFER);
            requests
                .try_send(AnalyzeStreamRequest {
                    msg: Some(analyze_stream_request::Msg::Options(analysis_options(
                        spec, layers,
                    ))),
                })
                .expect("fresh channel has capacity");
            let responses = client
                .analyze_stream(ReceiverStream::new(feed))
                .await?
                .into_inner();
            Ok(Self {
                submit: Some(AnalyzeSubmit {
                    requests: AnalyzeRequests::Sidecar(requests),
                }),
                responses: AnalyzeResponses::Sidecar(Box::new(responses)),
                vocab,
                layers,
            })
        }
    }

    fn open_native(
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        // The native analyzer serves the sentence layer itself (its
        // newline detector runs in the same pass, docs/highlighting.md);
        // every other optional layer needs the sidecar.
        let sidecar_only = SessionLayers {
            sentences: false,
            dual_cased: false,
            ..layers
        };
        if sidecar_only != SessionLayers::default() {
            let mut requested = Vec::new();
            if layers.quality {
                requested.push("quality");
            }
            if layers.geography {
                requested.push("geography");
            }
            if layers.entities {
                requested.push("entities");
            }
            return Err(Status::failed_precondition(format!(
                "native lexical analysis does not provide {} layers; configure an OpenNLP sidecar for this ingest",
                requested.join(", ")
            )));
        }
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let dual_cased = layers.dual_cased;
        let spec = std::sync::Arc::new(native_spec(spec)?);
        let (requests, mut feed) = tokio::sync::mpsc::channel::<NativeRequest>(SUBMIT_BUFFER);
        let (emit, responses) = tokio::sync::mpsc::channel::<NativeResponse>(SUBMIT_BUFFER);
        tokio::spawn(async move {
            while let Some(request) = feed.recv().await {
                let sequence = request.sequence;
                let spec = spec.clone();
                let result = tokio::task::spawn_blocking(move || {
                    if dual_cased {
                        native_dual_with_spec(&request.text, &spec)
                    } else {
                        native_analysis_with_spec(&request.text, &spec)
                    }
                })
                .await
                .unwrap_or_else(|error| {
                    Err(Status::internal(format!(
                        "native analysis worker failed: {error}"
                    )))
                });
                if emit
                    .send(NativeResponse { sequence, result })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::Native(requests),
            }),
            responses: AnalyzeResponses::Native(responses),
            vocab,
            layers,
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
        match &mut self.responses {
            AnalyzeResponses::Sidecar(responses) => match responses.message().await? {
                Some(response) => {
                    let sequence = response.sequence;
                    let result = match response.result {
                        Some(analyze_stream_response::Result::Ok(ok)) => {
                            // Vocabulary feed BEFORE the fold: `analyzed_from`
                            // drops the raw token texts the TOKENS channel
                            // counts. The feed never fails ingest.
                            if let Some(vocab) = &self.vocab {
                                vocab.feed(
                                    ok.term_vectors
                                        .iter()
                                        .map(|tv| (tv.term.as_str(), i64::from(tv.frequency))),
                                    ok.tokens.iter().map(|t| t.text.as_str()),
                                );
                            }
                            analyzed_from(ok, self.layers)
                        }
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
            },
            AnalyzeResponses::Native(responses) => match responses.recv().await {
                Some(response) => {
                    let result = response.result.map(|analysis| {
                        if let Some(vocab) = &self.vocab {
                            let body = &analysis.doc.fields[0];
                            vocab.feed(
                                body.terms.iter().map(|(term, frequency, _)| {
                                    (term.as_str(), i64::from(*frequency))
                                }),
                                analysis.tokens.iter().map(String::as_str),
                            );
                        }
                        analysis.doc
                    });
                    Ok(Some((response.sequence, result)))
                }
                None => Ok(None),
            },
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
    docs: &[(&str, Option<&AnalysisSpec>, SessionLayers)],
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
/// One batch session's key (spec and layers) and the input indices it serves.
type SessionGroup<'a> = ((Option<&'a AnalysisSpec>, SessionLayers), Vec<usize>);

pub async fn analyze_batch_streams(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>, SessionLayers)],
    streams: usize,
) -> Result<Vec<AnalyzedDoc>, Status> {
    let mut out: Vec<Option<AnalyzedDoc>> = Vec::new();
    out.resize_with(docs.len(), || None);
    // Group indices by spec, preserving first-seen order; the global doc
    // index is the sequence, so results land in their input slots no
    // matter which group or which stream answered.
    // One session per (spec, layers) pair: the layers a replayed record
    // needs (sentence spans, the cased identity) come from that one call.
    let mut groups: Vec<SessionGroup<'_>> = Vec::new();
    for (i, (_, spec, layers)) in docs.iter().enumerate() {
        match groups.iter_mut().find(|(key, _)| *key == (*spec, *layers)) {
            Some((_, indices)) => indices.push(i),
            None => groups.push(((*spec, *layers), vec![i])),
        }
    }
    for ((spec, layers), indices) in groups {
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
            sessions.push(open_stream(addr, spec, layers).await?);
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
async fn open_stream(
    addr: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzeStream, Status> {
    match AnalyzeStream::open_with_vocab(addr, spec, None, layers).await {
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

    fn noise_span(start: i32, end: i32, score: f64) -> crate::pb::analysis::NoiseSpan {
        crate::pb::analysis::NoiseSpan {
            span: Some(crate::pb::analysis::Span { start, end }),
            severity: "gibberish".to_string(),
            score,
        }
    }

    /// Overlapping and duplicate findings must count each damaged
    /// character once, in any emission order: `noise_chars` is the
    /// union's length, not the sum of the spans.
    #[test]
    fn noise_chars_is_the_union_not_the_sum() {
        let response = AnalyzeResponse {
            // [10,20) and [15,25) overlap; [15,25) repeats; [30,32) is
            // disjoint; [5,5) is empty and contributes nothing. Emitted
            // deliberately out of start order.
            noise: vec![
                noise_span(30, 32, 0.2),
                noise_span(15, 25, 0.4),
                noise_span(10, 20, 0.3),
                noise_span(15, 25, 0.4),
                noise_span(5, 5, 0.9),
            ],
            ..Default::default()
        };
        let q = doc_quality(&response);
        assert_eq!(q.noise_chars, 17, "union of [10,25) and [30,32)");
        assert_eq!(q.noise, 0.9, "worst score wins even from an empty span");
        assert_eq!(q.artifacts, 0);
    }

    /// A non-finite score is no signal, not the worst signal: it must
    /// not become the column value and poison min/max metadata.
    #[test]
    fn non_finite_noise_scores_are_ignored() {
        let response = AnalyzeResponse {
            noise: vec![noise_span(0, 4, f64::NAN), noise_span(4, 8, 0.5)],
            ..Default::default()
        };
        let q = doc_quality(&response);
        assert_eq!(q.noise, 0.5);
        assert_eq!(q.noise_chars, 8, "the NaN finding's SPAN still counts");
    }

    fn location(start: i32, confidence: f64, country: &str) -> crate::pb::analysis::GeoLocation {
        crate::pb::analysis::GeoLocation {
            span: Some(crate::pb::analysis::Span {
                start,
                end: start + 5,
            }),
            name: "Somewhere".to_string(),
            country_code: country.to_string(),
            latitude: f64::from(start),
            longitude: -f64::from(start),
            confidence,
        }
    }

    /// The point is the highest-confidence location; a tie keeps the
    /// FIRST in text order, so the reduction cannot depend on how the
    /// scan iterates. The country comes from the top region vote, not
    /// from the winning location.
    #[test]
    fn geography_picks_best_confidence_first_on_ties() {
        let response = AnalyzeResponse {
            locations: vec![
                location(0, 0.9, "FR"),
                location(10, 0.9, "DE"),
                location(20, 0.4, "US"),
            ],
            regions: vec![
                crate::pb::analysis::RegionVote {
                    country_code: "DE".to_string(),
                    share: 0.6,
                },
                crate::pb::analysis::RegionVote {
                    country_code: "FR".to_string(),
                    share: 0.4,
                },
            ],
            ..Default::default()
        };
        let g = doc_geography(&response);
        assert_eq!(g.point, Some((0.0, 0.0)), "tie keeps the first mention");
        assert_eq!(g.confidence, 0.9);
        assert_eq!(
            g.country, "DE",
            "the country is the aggregate vote, not the winning mention's"
        );
    }

    /// A non-finite confidence is no signal and cannot be chosen; a
    /// layer with ONLY such findings reduces to no point at all.
    #[test]
    fn non_finite_confidence_cannot_win() {
        let response = AnalyzeResponse {
            locations: vec![location(0, f64::NAN, "FR"), location(10, 0.3, "DE")],
            ..Default::default()
        };
        assert_eq!(doc_geography(&response).point, Some((10.0, -10.0)));

        let only_nan = AnalyzeResponse {
            locations: vec![location(0, f64::NAN, "FR")],
            ..Default::default()
        };
        assert_eq!(doc_geography(&only_nan).point, None);
    }

    /// No locations reduces to the all-absent value: no point, no
    /// country, confidence meaningless — the caller writes NOTHING,
    /// never a fabricated (0,0).
    #[test]
    fn no_locations_reduce_to_absence() {
        let g = doc_geography(&AnalyzeResponse::default());
        assert_eq!(g.point, None);
        assert_eq!(g.country, "");
    }

    /// A response with no findings measures exactly zero on every
    /// axis: "clean" is a measurement, distinct from "not measured"
    /// (which is `AnalyzedDoc::quality == None` and never reaches
    /// here).
    #[test]
    fn a_clean_response_measures_zero() {
        let q = doc_quality(&AnalyzeResponse::default());
        assert_eq!(q.noise, 0.0);
        assert_eq!(q.noise_chars, 0);
        assert_eq!(q.artifacts, 0);
    }

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
            occurrences: vec![Span { start: 0, end: 5 }],
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
            analyzed_from(with_empty, SessionLayers::default()).unwrap(),
            analyzed_from(without, SessionLayers::default()).unwrap(),
            "the empty term must vanish as if the analyzer never emitted it"
        );
    }

    #[test]
    fn search_refuses_a_sidecar_that_violates_its_utf16_storage_request() {
        let response = AnalyzeResponse {
            offset_unit: crate::pb::analysis::OffsetUnit::Utf8Bytes as i32,
            ..Default::default()
        };
        let error = analyzed_from(response, SessionLayers::default()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("ambiguous persisted spans"));
    }

    #[test]
    fn native_provider_runs_the_product_specs_with_utf16_offsets() {
        let folded =
            analyze_document_native("😀 Running Rodríguez running", Some(&body_spec())).unwrap();
        let body = &folded.fields[0];
        assert_eq!(body.length, 4);
        assert_eq!(
            body.terms,
            vec![
                ("😀".to_string(), 1, vec![(0, 2)]),
                ("run".to_string(), 2, vec![(3, 10), (21, 28)]),
                ("rodriguez".to_string(), 1, vec![(11, 20)]),
            ]
        );

        let cased = analyze_document_native("Running running", Some(&cased_body_spec())).unwrap();
        assert_eq!(
            cased.fields[0].terms,
            vec![
                ("Run".to_string(), 1, vec![(0, 7)]),
                ("run".to_string(), 1, vec![(8, 15)]),
            ]
        );
    }

    #[test]
    fn native_provider_refuses_unknown_and_server_defined_contracts() {
        let no_spec = analyze_document_native("court", None).unwrap_err();
        assert_eq!(no_spec.code(), tonic::Code::FailedPrecondition);
        assert!(no_spec.message().contains("explicit AnalysisSpec"));

        let mut unsupported = body_spec();
        unsupported.char_filters.push(CHAR_FILTER_NFKC);
        let error = analyze_document_native("court", Some(&unsupported)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error
            .message()
            .contains("does not implement normalizer step 12"));
    }

    #[tokio::test]
    async fn native_stream_uses_the_same_analysis_as_unary() {
        let mut stream = AnalyzeStream::open(NATIVE_ANALYSIS_BACKEND, Some(&body_spec()))
            .await
            .unwrap();
        let submit = stream.submitter();
        submit.submit(41, "running runs run").await.unwrap();
        drop(submit);
        stream.finish();
        let (sequence, streamed) = stream.next().await.unwrap().unwrap();
        assert_eq!(sequence, 41);
        assert_eq!(
            streamed.unwrap(),
            analyze_document_native("running runs run", Some(&body_spec())).unwrap()
        );
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn native_stream_keeps_document_errors_local() {
        let mut stream = AnalyzeStream::open(NATIVE_ANALYSIS_BACKEND, Some(&body_spec()))
            .await
            .unwrap();
        let submit = stream.submitter();
        submit.submit(1, "running").await.unwrap();
        submit.submit(2, "").await.unwrap();
        submit.submit(3, "appeals").await.unwrap();
        drop(submit);
        stream.finish();

        assert!(stream.next().await.unwrap().unwrap().1.is_ok());
        let error = stream.next().await.unwrap().unwrap().1.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(stream.next().await.unwrap().unwrap().1.is_ok());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn native_provider_refuses_sidecar_only_capabilities() {
        let embedding = embed_text(NATIVE_ANALYSIS_BACKEND, "court")
            .await
            .unwrap_err();
        assert_eq!(embedding.code(), tonic::Code::FailedPrecondition);

        let layer = AnalyzeStream::open_with_vocab(
            NATIVE_ANALYSIS_BACKEND,
            Some(&body_spec()),
            None,
            SessionLayers {
                dual_cased: false,
                sentences: false,
                quality: true,
                geography: false,
                entities: false,
            },
        )
        .await
        .err()
        .expect("quality must remain an OpenNLP capability");
        assert_eq!(layer.code(), tonic::Code::FailedPrecondition);
    }
}

#[cfg(all(test, feature = "net"))]
mod channel_pool_tests {
    use super::*;

    #[test]
    fn runtime_shutdown_releases_even_an_unpolled_pool_owner() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let weak = runtime.block_on(async {
            let first = runtime_channel_pool().unwrap();
            let second = runtime_channel_pool().unwrap();
            assert!(Arc::ptr_eq(&first, &second));
            let _channel = shared_channel("http://127.0.0.1:12345").unwrap();
            assert_eq!(first.lock().unwrap().len(), 1);
            Arc::downgrade(&first)
        });
        assert!(weak.upgrade().is_some(), "the runtime owns the idle pool");
        drop(runtime);
        assert!(
            weak.upgrade().is_none(),
            "shutdown must release cached channels"
        );
    }

    #[test]
    fn channels_outside_a_runtime_refuse_instead_of_panicking() {
        let error = shared_channel("http://127.0.0.1:12345").unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("Tokio runtime"));
    }
}
