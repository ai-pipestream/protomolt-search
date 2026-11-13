//! Pure-Rust lexical and one-pass NLP analysis providers backed by
//! `protomolt-analyzer` (`native`), in-process `turbo-client` (`turbo`), and
//! pooled `turbo-kserve` gRPC offload (`turbo.result.v1.AnalysisService`).
//!
//! Search configures every backend to persist original-text UTF-8 byte offsets
//! (`ViewId::SOURCE`), while preserving bidirectional UTF-8 / UTF-16 / Unicode
//! code-point span projection through `turbo-client`.

#[path = "analyzer_wire.rs"]
pub(crate) mod wire;

#[cfg(feature = "net")]
use std::collections::HashMap;
#[cfg(feature = "net")]
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

#[cfg(feature = "net")]
use tokio_stream::wrappers::ReceiverStream;
#[cfg(feature = "net")]
use tonic::transport::Channel;
use tonic::Status;
#[cfg(feature = "net")]
use tonic::Streaming;

use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Matches `turbo-kserve`'s default per-document text byte cap.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Configuration value selecting the minimal in-process `protomolt-analyzer` backend.
pub const NATIVE_ANALYSIS_BACKEND: &str = "native";

/// Configuration value selecting the full in-process pure-Rust `turbo-client`
/// lexical lattice + one-pass NLP backend (zero network, zero JVM).
pub const TURBO_ANALYSIS_BACKEND: &str = "turbo";

/// Default embedding model name queried by [`embed_text`] when the endpoint URI
/// does not specify `?model=<name>` and `TURBO_EMBED_MODEL` is unset.
pub const DEFAULT_EMBED_MODEL: &str = "embed";

/// Resolved analysis provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisBackend {
    /// Minimal in-process `protomolt-analyzer` lexical provider.
    Native,
    /// Full in-process `turbo-client` lexical lattice + one-pass NLP provider.
    Turbo,
    /// Pooled remote `turbo-kserve` (or `MockAnalysis`) gRPC endpoint.
    Sidecar(String),
}

impl AnalysisBackend {
    pub fn parse(value: &str) -> Result<Self, Status> {
        match value.trim() {
            "native" | "native://" => Ok(Self::Native),
            "turbo" | "turbo://" => Ok(Self::Turbo),
            "" => Err(Status::invalid_argument("analysis backend is empty")),
            address => Ok(Self::Sidecar(address.to_string())),
        }
    }
}

/// Optional per-call remote scheduling, auth, TLS, and deadline overrides
/// propagated to `turbo-kserve` (`#105`).
#[derive(Clone, Debug, Default)]
pub struct RemoteCallOptions {
    /// Explicit bearer token (`authorization: Bearer <token>`).
    pub bearer_token: Option<String>,
    /// Interactive vs bulk scheduling lane (`x-turbo-priority`).
    pub interactive: bool,
    /// Optional per-RPC deadline translated into `grpc-timeout`.
    pub timeout: Option<Duration>,
    /// Optional explicit mTLS / server CA configuration (falls back to
    /// [`crate::security::process_client_tls`]).
    pub tls: Option<crate::security::ClientTls>,
}

/// The analysis a body-text corpus is built with, and the ONLY spec that
/// may be used to query one.
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

/// Unicode word-boundary variant of [`body_spec`].
pub fn uax29_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_UAX29,
        ..body_spec()
    }
}

/// The cased twin of [`body_spec`]: identical tokenizer and stemmer,
/// but term identity taken from the stem of the SURFACE form.
pub fn cased_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        term_vector_source: SOURCE_STEMS,
        char_filters: Vec::new(),
        ..body_spec()
    }
}

/// Named corpus analyzers for CLIs and configuration.
pub fn analyzer_by_name(name: &str) -> Result<Option<AnalysisSpec>, String> {
    Ok(match name {
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

/// Stable 64-bit FNV-1a identity of an [`AnalysisSpec`] persisted in `.bm25` headers.
pub fn analysis_fingerprint(spec: Option<&AnalysisSpec>) -> u64 {
    fn eat(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    let Some(spec) = spec else { return 0 };
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    eat(&mut hash, b"turbovec.analysis.v1");
    for value in [
        spec.tokenizer,
        spec.stemmer,
        spec.term_vector_mode,
        spec.term_vector_source,
    ] {
        eat(&mut hash, &value.to_le_bytes());
    }
    eat(&mut hash, &(spec.char_filters.len() as u32).to_le_bytes());
    for filter in &spec.char_filters {
        eat(&mut hash, &filter.to_le_bytes());
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

/// Cryptographic SHA-256 [`turbo_client::IndexGenerationHandshake`] combining
/// a field's [`turbo_client::AnalysisFingerprint`] with an optional neural
/// [`turbo_client::BundleScoringFingerprint`].
pub fn turbo_handshake(
    spec: Option<&AnalysisSpec>,
    bundle: Option<turbo_client::BundleScoringFingerprint>,
) -> Result<turbo_client::IndexGenerationHandshake, Status> {
    let cfg = turbo_field_config(spec)?;
    Ok(turbo_client::IndexGenerationHandshake {
        analysis: Some(cfg.fingerprint()),
        bundle,
    })
}

/// `Tokenizer.TOKENIZER_WHITESPACE`.
pub const TOKENIZER_WHITESPACE: i32 = 1;
/// `Tokenizer.TOKENIZER_SIMPLE`.
pub const TOKENIZER_SIMPLE: i32 = 2;
/// `Tokenizer.TOKENIZER_UAX29`.
pub const TOKENIZER_UAX29: i32 = 3;
/// `Stemmer.STEMMER_NONE`.
pub const STEMMER_NONE: i32 = 1;
/// `Stemmer.STEMMER_PORTER`.
pub const STEMMER_PORTER: i32 = 2;
/// `TermVectorMode.MODE_FULL` (occurrence offsets included).
pub const TERM_VECTOR_MODE_FULL: i32 = 1;
/// `TermVectorMode.MODE_SCORING_ONLY` (frequency only).
pub const TERM_VECTOR_MODE_SCORING_ONLY: i32 = 2;
/// `TermVectorSource.SOURCE_TOKENS` (char filters define identity).
pub const SOURCE_TOKENS: i32 = 1;
/// `TermVectorSource.SOURCE_STEMS` (char filters ignored).
pub const SOURCE_STEMS: i32 = 2;
/// `TermVectorSource.SOURCE_NORMALIZED_STEMS` (char filters, then stem).
pub const SOURCE_NORMALIZED_STEMS: i32 = 3;
/// Strip invisible controls.
pub const CHAR_FILTER_STRIP_INVISIBLE: i32 = 1;
/// Collapse whitespace.
pub const CHAR_FILTER_WHITESPACE: i32 = 2;
/// Full Unicode case fold.
pub const CHAR_FILTER_FULL_CASE_FOLD: i32 = 6;
/// Unicode NFKC compatibility composition.
pub const CHAR_FILTER_NFKC: i32 = 12;
/// Unicode NFC canonical composition.
pub const CHAR_FILTER_NFC: i32 = 13;
/// Strip diacritics (`Rodríguez` -> `Rodriguez`).
pub const CHAR_FILTER_ACCENT_FOLD: i32 = 15;
/// Simple (per-character) case folding.
pub const CHAR_FILTER_CASE_FOLD: i32 = 16;
/// Strip HTML/XML markup tags while preserving source offsets.
pub const CHAR_FILTER_HTML_STRIP: i32 = 20;
/// Decode HTML character entities while preserving source offsets.
pub const CHAR_FILTER_HTML_ENTITY: i32 = 21;
/// Repair UTF-8-as-Latin-1 mojibake sequences while preserving source offsets.
pub const CHAR_FILTER_MOJIBAKE: i32 = 23;
/// Rejoin words split across line-break hyphens while emitting split+joined lattice arcs.
pub const CHAR_FILTER_DEHYPHENATE: i32 = 24;

/// Normalize a term prefix under `spec`'s normalizer chain (never stemming).
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
             an undeclared chain is not known to the coordinator",
        ));
    };
    if spec.term_vector_source == SOURCE_STEMS {
        return Ok(prefix.to_string());
    }
    let normalized = match native_spec(Some(spec)) {
        Ok(native) => protomolt_analyzer::normalize_term(prefix, &native.normalizers),
        Err(_) => {
            let cfg = turbo_field_config(Some(spec))?;
            turbo_client::LexicalAnalyzer::new(cfg).normalize_prefix(prefix)
        }
    };
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
#[cfg(feature = "net")]
static TURBO_REMOTE_POOL: OnceLock<turbo_client::RemoteChannelPool> = OnceLock::new();

#[cfg(feature = "net")]
pub(crate) fn shared_turbo_pool() -> turbo_client::RemoteChannelPool {
    TURBO_REMOTE_POOL
        .get_or_init(turbo_client::RemoteChannelPool::new)
        .clone()
}

/// Build a pooled [`turbo_client::RemoteClient`] honoring `options` and
/// process-wide TLS/bearer settings.
#[cfg(feature = "net")]
pub fn remote_client_for(
    addr: &str,
    options: &RemoteCallOptions,
) -> Result<turbo_client::RemoteClient, Status> {
    let tls_ref = options
        .tls
        .as_ref()
        .or_else(|| crate::security::process_client_tls());
    let endpoint_uri = if tls_ref.is_some() {
        crate::security::secure_url(addr, tls_ref)
    } else {
        addr.to_string()
    };
    let priority = if options.interactive {
        turbo_client::RemotePriority::Interactive
    } else {
        turbo_client::RemotePriority::Bulk
    };
    let mut cfg = turbo_client::RemoteEndpointConfig::new(endpoint_uri).with_priority(priority);
    if let Some(token) = options.bearer_token.clone().or_else(|| {
        std::env::var("TURBO_BEARER_TOKEN")
            .ok()
            .filter(|s| !s.trim().is_empty())
    }) {
        cfg = cfg.with_bearer_token(token);
    }
    if let Some(timeout) = options.timeout {
        cfg = cfg.with_timeout(timeout);
    }
    #[cfg(feature = "tls")]
    if let Some(tls) = tls_ref {
        cfg = cfg.with_tls(turbo_client::RemoteTlsConfig {
            server_ca_pem: tls.ca_pem.clone(),
            client_identity_pem: tls.identity_pem.clone(),
            domain_name: tls.domain.clone(),
        });
    }
    #[cfg(not(feature = "tls"))]
    if tls_ref.is_some() {
        return Err(Status::failed_precondition(
            "this build has no TLS support (feature `tls` is off)",
        ));
    }
    Ok(turbo_client::RemoteClient::with_pool(
        cfg,
        shared_turbo_pool(),
    ))
}

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
    let owner = Arc::clone(&pool);
    handle.spawn(async move {
        std::future::pending::<()>().await;
        drop(owner);
    });
    Ok(pool)
}

/// Reuse an analysis channel within the current Tokio runtime.
#[cfg(feature = "net")]
pub fn shared_channel(addr: &str) -> Result<Channel, Status> {
    let pool = runtime_channel_pool()?;
    let mut channels = pool.lock().expect("analysis channel pool poisoned");
    if let Some(channel) = channels.get(addr) {
        return Ok(channel.clone());
    }
    let tls = crate::security::process_client_tls();
    let url = if tls.is_some() {
        crate::security::secure_url(addr, tls)
    } else {
        addr.to_string()
    };
    let endpoint = Channel::from_shared(url)
        .map_err(|error| {
            Status::invalid_argument(format!("bad analysis address {addr:?}: {error}"))
        })?
        .tcp_nodelay(true);
    let endpoint =
        crate::security::apply_client_tls(endpoint, tls).map_err(Status::failed_precondition)?;
    let channel = endpoint.connect_lazy();
    channels.insert(addr.to_string(), channel.clone());
    Ok(channel)
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
    #[cfg(not(feature = "net"))]
    {
        let _ = (spec, layers, options);
        Err(sidecar_needs_net(addr))
    }
    #[cfg(feature = "net")]
    {
        let remote = remote_client_for(addr, options)?;
        let ch = remote
            .pool()
            .channel(remote.config())
            .map_err(client_err_to_status)?;
        let mut client = AnalysisServiceClient::new(ch)
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
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
        let prio = if options.interactive {
            turbo_client::RemotePriority::Interactive
        } else {
            turbo_client::RemotePriority::Bulk
        };
        let req = remote
            .prepare_request(payload, Some(prio), options.timeout)
            .map_err(client_err_to_status)?;
        let mut stream = client.analyze_document_progressive(req).await?.into_inner();
        let mut graph_resp: Option<DocumentGraphResponse> = None;
        while let Some(ev) = stream.message().await? {
            if let Some(analyze_progressive_event::Event::Complete(done)) = ev.event {
                graph_resp = Some(done);
            }
        }
        let done = graph_resp.ok_or_else(|| {
            Status::internal("AnalyzeDocumentProgressive stream ended without a Complete event")
        })?;
        decode_graph_response(done, spec, layers, None)
    }
}

/// Analyze one document with the minimal in-process `protomolt-analyzer` provider.
pub fn analyze_document_native(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    Ok(native_analysis(text, spec)?.doc)
}

/// Both identities from one native pass (`docs/dual-cased.md`).
pub fn analyze_document_native_dual(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    validate_dual_cased_spec(spec)?;
    let spec = native_spec(spec)?;
    Ok(native_dual_with_spec(text, &spec)?.doc)
}

fn analyze_document_native_layers(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    let unsupported = SessionLayers {
        sentences: false,
        dual_cased: false,
        ..layers
    };
    if unsupported != SessionLayers::default() {
        return Err(Status::failed_precondition(
            "native lexical analysis does not provide quality, geography, or entities; use --analysis-addr=turbo or turbo-kserve",
        ));
    }
    let mut doc = if layers.dual_cased {
        analyze_document_native_dual(text, spec)?
    } else {
        analyze_document_native(text, spec)?
    };
    if !layers.sentences {
        // Keep native's default behavior (sentences populated) consistent with analyze_document_native.
        let _ = &mut doc;
    }
    Ok(doc)
}

/// Analyze one document in-process using `turbo-client` (`LexicalAnalyzer` +
/// optional `OnePassDocumentNlp` layers) with zero network calls and zero JVM.
pub fn analyze_document_turbo(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    Ok(turbo_analysis_pass(text, spec, layers)?.doc)
}

struct TurboPassOutput {
    doc: AnalyzedDoc,
    surface_tokens: Vec<String>,
}

static TURBO_NLP_SINGLETON: std::sync::OnceLock<Result<turbo_client::OnePassDocumentNlp, String>> =
    std::sync::OnceLock::new();

fn shared_turbo_nlp() -> Result<&'static turbo_client::OnePassDocumentNlp, Status> {
    TURBO_NLP_SINGLETON
        .get_or_init(|| {
            turbo_client::OnePassDocumentNlp::builder()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|msg| Status::internal(format!("failed to build OnePassDocumentNlp: {msg}")))
}

fn turbo_analysis_pass(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<TurboPassOutput, Status> {
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
    if layers.dual_cased {
        validate_dual_cased_spec(spec)?;
    }
    let effective_spec = spec.cloned().unwrap_or_else(body_spec);
    let scoring_only = effective_spec.term_vector_mode == TERM_VECTOR_MODE_SCORING_ONLY;
    let cfg = turbo_field_config(Some(&effective_spec))?;
    let lex = turbo_client::LexicalAnalyzer::new(cfg);
    let surface_tokens: Vec<String> = text.split_whitespace().map(str::to_owned).collect();

    let (primary_field, cased_field) = if layers.dual_cased {
        let dual = lex.analyze_dual(text).map_err(client_err_to_status)?;
        (
            turbo_field_to_postings(dual.primary, scoring_only),
            Some(turbo_field_to_postings(dual.cased, scoring_only)),
        )
    } else {
        let primary = lex.analyze(text).map_err(client_err_to_status)?;
        (turbo_field_to_postings(primary, scoring_only), None)
    };

    let (quality, geography, entities, nlp_proj) =
        if layers.quality || layers.geography || layers.entities || layers.nlp_columns {
            let nlp = shared_turbo_nlp()?
                .analyze(text)
                .map_err(client_err_to_status)?;
            let (q, g, e) = reduce_nlp_layers(&nlp, layers);
            (q, g, e, layers.nlp_columns.then(|| Box::new(nlp)))
        } else {
            (None, None, Vec::new(), None)
        };

    Ok(TurboPassOutput {
        doc: AnalyzedDoc {
            fields: vec![primary_field],
            cased: cased_field,
            quality,
            geography,
            entities,
            nlp: nlp_proj,
        },
        surface_tokens,
    })
}

fn reduce_nlp_layers(
    nlp: &turbo_client::DocumentNlpProjection,
    layers: SessionLayers,
) -> (
    Option<crate::postings::DocQuality>,
    Option<crate::postings::DocGeography>,
    Vec<crate::phrases::DocEntity>,
) {
    let quality = layers.quality.then(|| crate::postings::DocQuality {
        noise: if nlp.quality.worst_noise_score.is_finite() {
            nlp.quality.worst_noise_score
        } else {
            0.0
        },
        noise_chars: nlp.quality.union_noise_chars,
        artifacts: nlp.quality.artifact_count,
    });
    let geography = layers.geography.then(|| {
        let mut best: Option<&turbo_client::GeoHit> = None;
        let mut votes: Vec<(String, usize)> = Vec::new();
        for hit in &nlp.geo {
            if !hit.confidence.is_finite() {
                continue;
            }
            if best.is_none_or(|b| hit.confidence > b.confidence) {
                best = Some(hit);
            }
            if !hit.country_code.is_empty() {
                match votes.iter_mut().find(|(c, _)| *c == hit.country_code) {
                    Some((_, n)) => *n += 1,
                    None => votes.push((hit.country_code.clone(), 1)),
                }
            }
        }
        votes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        crate::postings::DocGeography {
            point: best.map(|b| (b.latitude, b.longitude)),
            confidence: best.map_or(0.0, |b| b.confidence),
            country: votes.first().map(|(c, _)| c.clone()).unwrap_or_default(),
        }
    });
    let entities = if layers.entities {
        nlp.affirmed_entities
            .iter()
            .map(|e| crate::phrases::DocEntity {
                kind: e.kind.clone(),
                text: e.surface.clone(),
                start: e.spans.utf8.0,
                end: e.spans.utf8.1,
            })
            .collect()
    } else {
        Vec::new()
    };
    (quality, geography, entities)
}

fn turbo_field_to_postings(
    analyzed: turbo_client::AnalyzedField,
    scoring_only: bool,
) -> crate::postings::AnalyzedField {
    let mut term_indices: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut terms: crate::postings::DocTerms = Vec::new();
    let mut positions: crate::postings::DocPositions = Vec::new();
    let mut pos_lengths: crate::postings::DocPositions = Vec::new();
    let mut seen_ordinals: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut has_lattice = false;
    let mut primary_len = 0u32;
    let mut max_tf = 0u32;

    for arc in analyzed.arcs {
        if arc.term.is_empty() {
            continue;
        }
        let arc_len = arc.pos_len.max(1);
        if arc_len == 1 {
            primary_len = primary_len.saturating_add(1);
        } else {
            has_lattice = true;
        }
        let span = arc.spans.utf8;
        match term_indices.get(&arc.term).copied() {
            Some(idx) => {
                // Keep per-term positions strictly ascending: if a normalized/compound
                // arc lands on an already-recorded position for the identical term,
                // keep the wider source span and arc span rather than duplicating the ordinal.
                if let Some(last_pos) = positions[idx].last().copied() {
                    if arc.pos <= last_pos {
                        if arc.pos == last_pos && !scoring_only {
                            if let Some(last_occ) = terms[idx].2.last_mut() {
                                last_occ.0 = last_occ.0.min(span.0);
                                last_occ.1 = last_occ.1.max(span.1);
                            }
                            if let Some(last_len) = pos_lengths[idx].last_mut() {
                                *last_len = (*last_len).max(arc_len);
                            }
                        }
                        continue;
                    }
                }
                if !seen_ordinals.insert(arc.pos) {
                    has_lattice = true;
                }
                terms[idx].1 += 1;
                max_tf = max_tf.max(terms[idx].1);
                if !scoring_only {
                    terms[idx].2.push(span);
                }
                positions[idx].push(arc.pos);
                pos_lengths[idx].push(arc_len);
            }
            None => {
                if !seen_ordinals.insert(arc.pos) {
                    has_lattice = true;
                }
                let idx = terms.len();
                term_indices.insert(arc.term.clone(), idx);
                max_tf = max_tf.max(1);
                let occs = if scoring_only { Vec::new() } else { vec![span] };
                terms.push((arc.term, 1, occs));
                positions.push(vec![arc.pos]);
                pos_lengths.push(vec![arc_len]);
            }
        }
    }

    let mut raw_sentences: Vec<(u32, u32)> = analyzed
        .sentences
        .iter()
        .map(|s| s.spans.utf8)
        .filter(|&(s, e)| s < e)
        .collect();
    // Ensure any multi-line dehyphenated occurrence span bridging two adjacent
    // sentences is enclosed by a single merged sentence span so `check_sentences`
    // holds unconditionally.
    for (_, _, occs) in &terms {
        for &(os, oe) in occs {
            if os >= oe {
                continue;
            }
            let first = raw_sentences.partition_point(|&(_, se)| se <= os);
            let last_excl = raw_sentences.partition_point(|&(ss, _)| ss < oe);
            if first < raw_sentences.len() && last_excl > first + 1 {
                let merged_start = raw_sentences[first].0.min(os);
                let merged_end = raw_sentences[last_excl - 1].1.max(oe);
                raw_sentences.splice(first..last_excl, [(merged_start, merged_end)]);
            } else if first < raw_sentences.len() {
                raw_sentences[first].0 = raw_sentences[first].0.min(os);
                raw_sentences[first].1 = raw_sentences[first].1.max(oe);
            } else if os < oe {
                raw_sentences.push((os, oe));
            }
        }
    }

    crate::postings::AnalyzedField {
        terms,
        length: primary_len.max(max_tf),
        positions: if scoring_only { None } else { Some(positions) },
        pos_lengths: if !scoring_only && has_lattice {
            Some(pos_lengths)
        } else {
            None
        },
        sentences: Some(raw_sentences),
        ..Default::default()
    }
}

/// Translate a `protomolt-search` [`AnalysisSpec`] into a `turbo-client`
/// [`turbo_client::FieldAnalysisConfig`].
pub fn turbo_field_config(
    spec: Option<&AnalysisSpec>,
) -> Result<turbo_client::FieldAnalysisConfig, Status> {
    use turbo_client::{NormalizerKind, StemmerKind, TermSourceKind, TokenizerKind};

    let effective = spec.cloned().unwrap_or_else(body_spec);
    let tokenizer = match effective.tokenizer {
        0 | TOKENIZER_WHITESPACE => TokenizerKind::Whitespace,
        TOKENIZER_SIMPLE => TokenizerKind::Simple,
        TOKENIZER_UAX29 => TokenizerKind::Uax29,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported tokenizer {other}; expected WHITESPACE (1), SIMPLE (2), or UAX29 (3)"
            )))
        }
    };
    let stemmer = match effective.stemmer {
        0 | STEMMER_NONE => StemmerKind::None,
        STEMMER_PORTER => StemmerKind::Porter,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported stemmer {other}"
            )))
        }
    };
    let term_source = match effective.term_vector_source {
        0 | SOURCE_TOKENS => TermSourceKind::Tokens,
        SOURCE_STEMS => TermSourceKind::Stems,
        SOURCE_NORMALIZED_STEMS => TermSourceKind::NormalizedStems,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported term_vector_source {other}"
            )))
        }
    };
    if matches!(
        term_source,
        TermSourceKind::Stems | TermSourceKind::NormalizedStems
    ) && stemmer == StemmerKind::None
    {
        return Err(Status::invalid_argument(
            "term vector source STEMS requires a stemmer other than STEMMER_NONE",
        ));
    }
    let mut normalizers = Vec::new();
    if term_source != TermSourceKind::Stems {
        for &step in &effective.char_filters {
            let kind = match step {
                0 => continue,
                CHAR_FILTER_STRIP_INVISIBLE => NormalizerKind::StripInvisible,
                CHAR_FILTER_WHITESPACE => NormalizerKind::Whitespace,
                CHAR_FILTER_FULL_CASE_FOLD => NormalizerKind::FullCaseFold,
                CHAR_FILTER_NFKC => NormalizerKind::Nfkc,
                CHAR_FILTER_NFC => NormalizerKind::Nfc,
                CHAR_FILTER_ACCENT_FOLD => NormalizerKind::AccentFold,
                CHAR_FILTER_CASE_FOLD => NormalizerKind::SimpleCaseFold,
                CHAR_FILTER_HTML_STRIP => NormalizerKind::HtmlTagStrip,
                CHAR_FILTER_HTML_ENTITY => NormalizerKind::HtmlEntityDecode,
                CHAR_FILTER_MOJIBAKE => NormalizerKind::Mojibake,
                CHAR_FILTER_DEHYPHENATE => NormalizerKind::Dehyphenate,
                unsupported => {
                    return Err(Status::invalid_argument(format!(
                        "unsupported normalizer step {unsupported}"
                    )))
                }
            };
            normalizers.push(kind);
        }
    }
    Ok(turbo_client::FieldAnalysisConfig {
        tokenizer,
        normalizers,
        stemmer,
        term_source,
        emit_lattice: true,
        sentence_language: "eng".to_string(),
    })
}

fn client_err_to_status(err: turbo_client::ClientError) -> Status {
    match err {
        turbo_client::ClientError::InvalidArgument(m) => Status::invalid_argument(m),
        turbo_client::ClientError::FailedPrecondition(m) => Status::failed_precondition(m),
        turbo_client::ClientError::NotFound(m) => Status::not_found(m),
        turbo_client::ClientError::PermissionDenied(m) => Status::permission_denied(m),
        turbo_client::ClientError::DeadlineExceeded(m) => Status::deadline_exceeded(m),
        turbo_client::ClientError::Unauthenticated(m) => Status::unauthenticated(m),
        turbo_client::ClientError::Unavailable(m) => Status::unavailable(m),
        turbo_client::ClientError::Internal(m) => Status::internal(m),
    }
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
            ..Default::default()
        },
        tokens,
    })
}

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
            ..Default::default()
        },
        tokens,
    })
}

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
        ..Default::default()
    }
}

pub(crate) fn validate_native_spec(spec: &AnalysisSpec) -> Result<(), Status> {
    native_spec(Some(spec)).map(|_| ())
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

#[cfg(not(feature = "net"))]
fn sidecar_needs_net(addr: &str) -> Status {
    Status::failed_precondition(format!(
        "analysis service {addr:?} is unreachable from this build: the `net` feature is off, so \
         nothing dials; use --analysis-addr=native or --analysis-addr=turbo"
    ))
}

#[cfg(feature = "net")]
use turbo_client::proto::result::analysis_service_client::AnalysisServiceClient;
#[cfg(feature = "net")]
use turbo_client::proto::result::{
    analyze_progressive_event, analyze_stream_request, analyze_stream_response, AnalyzeDocument,
    AnalyzeOrder, AnalyzeOutput, AnalyzePipelineRequest, AnalyzeStreamHeader, AnalyzeStreamRequest,
    AnalyzeStreamResponse, CapabilitiesRequest, DocumentGraphResponse, OffsetMode,
    PiiOutputOptions,
};

#[cfg(feature = "net")]
fn decode_graph_response(
    response: DocumentGraphResponse,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
    vocab: Option<&Arc<crate::vocab::VocabularyListener>>,
) -> Result<AnalyzedDoc, Status> {
    if response.graph.starts_with(wire::MOCK_GRAPH_MAGIC) {
        let mock = wire::MockAnalyzeResponse::decode_graph(&response.graph)?;
        if let Some(vocab) = vocab {
            vocab.feed(
                mock.term_vectors
                    .iter()
                    .map(|tv| (tv.term.as_str(), i64::from(tv.frequency))),
                mock.tokens.iter().map(|t| t.text.as_str()),
            );
        }
        return wire::analyzed_from_mock(mock, layers);
    }
    if response.source_text.is_empty() && response.graph.is_empty() {
        return Err(Status::failed_precondition(
            "turbo-kserve returned an empty DocumentGraphResponse (check PII output disclosure)",
        ));
    }
    let out = turbo_analysis_pass(&response.source_text, spec, layers)?;
    if let Some(vocab) = vocab {
        let body = &out.doc.fields[0];
        vocab.feed(
            body.terms
                .iter()
                .map(|(term, freq, _)| (term.as_str(), i64::from(*freq))),
            out.surface_tokens.iter().map(String::as_str),
        );
    }
    Ok(out.doc)
}

/// Embed `text` through `turbo-kserve`'s Open Inference Protocol (`ModelInfer`)
/// into one L2-normalized query vector.
pub async fn embed_text(addr: &str, text: &str) -> Result<Vec<f32>, Status> {
    match AnalysisBackend::parse(addr)? {
        AnalysisBackend::Native | AnalysisBackend::Turbo => {
            return Err(Status::failed_precondition(
                "in-process lexical analysis does not provide neural embeddings without a configured model bundle; configure a turbo-kserve endpoint or local embedding provider",
            ));
        }
        AnalysisBackend::Sidecar(_) => {}
    }
    if text.is_empty() {
        return Err(Status::invalid_argument("empty text"));
    }
    #[cfg(not(feature = "net"))]
    {
        Err(sidecar_needs_net(addr))
    }
    #[cfg(feature = "net")]
    {
        let (clean_addr, model_name) = split_model_query_param(addr);
        let remote = remote_client_for(
            &clean_addr,
            &RemoteCallOptions {
                interactive: true,
                ..Default::default()
            },
        )?;
        let (mut vec, dim) = remote
            .embed_texts(
                &model_name,
                &[text],
                Some("query"),
                Some(turbo_client::RemotePriority::Interactive),
                None,
            )
            .await
            .map_err(client_err_to_status)?;
        if vec.is_empty() || dim == 0 {
            return Err(Status::failed_precondition(
                "turbo-kserve returned no embeddings",
            ));
        }
        vec.truncate(dim);
        let norm = vec
            .iter()
            .map(|&v| f64::from(v) * f64::from(v))
            .sum::<f64>()
            .sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return Err(Status::failed_precondition("embedding pooled to zero"));
        }
        for v in &mut vec {
            *v = (f64::from(*v) / norm) as f32;
        }
        Ok(vec)
    }
}

#[cfg(feature = "net")]
fn split_model_query_param(addr: &str) -> (String, String) {
    if let Some((base, query)) = addr.split_once('?') {
        for pair in query.split('&') {
            if let Some(("model", value)) = pair.split_once('=') {
                if !value.trim().is_empty() {
                    return (base.to_string(), value.trim().to_string());
                }
            }
        }
        return (base.to_string(), env_embed_model());
    }
    (addr.to_string(), env_embed_model())
}

#[cfg(feature = "net")]
fn env_embed_model() -> String {
    std::env::var("TURBO_EMBED_MODEL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string())
}

/// Optional layers an analysis session requests beyond primary term identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionLayers {
    /// Noise and artifact scanners (`docs/quality-columns.md`).
    pub quality: bool,
    /// Geocoding layer (`docs/geography-columns.md`).
    pub geography: bool,
    /// Named-entity mentions for materialization into a map column.
    pub entities: bool,
    /// Sentence spans in original-text UTF-8 coordinates (`docs/highlighting.md`).
    pub sentences: bool,
    /// Both folded and cased term identities from one pass (`docs/dual-cased.md`).
    pub dual_cased: bool,
    /// Full one-pass `DocumentNlpProjection` retained for pre-WAL typed column materialization (`#103`).
    pub nlp_columns: bool,
}

/// Zero-length terms dropped at the analysis boundary since process start.
#[cfg(any(feature = "net", test))]
pub(crate) static EMPTY_TERMS_DROPPED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cased twin of an [`AnalysisSpec`]: identical tokenizer, stemmer, mode, and
/// source with case-folding filters removed (`docs/dual-cased.md`).
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

/// Validate that `spec` can produce a distinct folded/cased twin pair.
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

/// Client-side submission buffer of an [`AnalyzeStream`].
const SUBMIT_BUFFER: usize = 32;

/// Cloneable submission handle for an open [`AnalyzeStream`].
#[derive(Clone)]
pub struct AnalyzeSubmit {
    requests: AnalyzeRequests,
}

#[derive(Clone)]
enum AnalyzeRequests {
    #[cfg(feature = "net")]
    Remote {
        tx: tokio::sync::mpsc::Sender<AnalyzeStreamRequest>,
        binding: turbo_client::proto::result::InputBinding,
    },
    InProcess(tokio::sync::mpsc::Sender<InProcessRequest>),
}

struct InProcessRequest {
    sequence: u64,
    text: String,
}

struct InProcessResponse {
    sequence: u64,
    result: Result<InProcessItem, Status>,
}

struct InProcessItem {
    doc: AnalyzedDoc,
    surface_tokens: Vec<String>,
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
    submit: Option<AnalyzeSubmit>,
    responses: AnalyzeResponses,
    vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
    spec: Option<AnalysisSpec>,
    layers: SessionLayers,
}

enum AnalyzeResponses {
    #[cfg(feature = "net")]
    Remote(Box<Streaming<AnalyzeStreamResponse>>),
    InProcess(tokio::sync::mpsc::Receiver<InProcessResponse>),
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

    /// Opens an [`AnalyzeStream`] session with explicit [`RemoteCallOptions`]
    /// (defaulting to `RemotePriority::Bulk` for ingest/replay streams).
    pub async fn open_with_options(
        addr: &str,
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
        options: &RemoteCallOptions,
    ) -> Result<Self, Status> {
        match AnalysisBackend::parse(addr)? {
            AnalysisBackend::Native => return Self::open_native(spec, vocab, layers),
            AnalysisBackend::Turbo => return Self::open_turbo(spec, vocab, layers),
            AnalysisBackend::Sidecar(_) => {}
        }
        #[cfg(not(feature = "net"))]
        {
            let _ = (spec, vocab, layers, options);
            Err(sidecar_needs_net(addr))
        }
        #[cfg(feature = "net")]
        {
            if layers.dual_cased {
                validate_dual_cased_spec(spec)?;
            }
            let remote = remote_client_for(addr, options)?;
            let ch = remote
                .pool()
                .channel(remote.config())
                .map_err(client_err_to_status)?;
            let mut client = AnalysisServiceClient::new(ch)
                .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);

            if layers.geography || layers.entities || layers.dual_cased {
                let cap_req = remote
                    .prepare_request(
                        CapabilitiesRequest {},
                        Some(turbo_client::RemotePriority::Interactive),
                        options.timeout,
                    )
                    .map_err(client_err_to_status)?;
                let capabilities = client.capabilities(cap_req).await?.into_inner();
                let ner_available = capabilities.profiles.iter().any(|p| {
                    p.layers
                        .iter()
                        .any(|l| l == "turbo:entities" || l == "turbo:geo")
                });
                if (layers.geography || layers.entities) && !ner_available {
                    return Err(Status::failed_precondition(
                        "entity or geography columns were requested but this analysis server has no NER model configured (Capabilities.ner_available = false); configure an NER model or disable those columns",
                    ));
                }
                let dual_available = !capabilities
                    .profiles
                    .iter()
                    .any(|p| p.name == wire::NO_DUAL_CASED_PROFILE);
                if layers.dual_cased && !dual_available {
                    return Err(Status::failed_precondition(format!(
                        "a cased field was requested but the analysis server at {addr} does not serve the \
                         dual term identity (dual_term_identity_available = false); \
                         upgrade turbo-kserve or drop cased_field"
                    )));
                }
            }

            let session = wire::WireSessionOptions {
                spec: spec.cloned(),
                layers,
            };
            let binding = session.to_binding();
            let (tx, feed) = tokio::sync::mpsc::channel(SUBMIT_BUFFER);
            tx.try_send(AnalyzeStreamRequest {
                kind: Some(analyze_stream_request::Kind::Header(AnalyzeStreamHeader {
                    order: AnalyzeOrder::Completion as i32,
                    max_inflight: 0,
                    batch_linger_ms: 0,
                })),
            })
            .expect("fresh channel has capacity");

            let prio = if options.interactive {
                turbo_client::RemotePriority::Interactive
            } else {
                turbo_client::RemotePriority::Bulk
            };
            let stream_req = remote
                .prepare_request(ReceiverStream::new(feed), Some(prio), options.timeout)
                .map_err(client_err_to_status)?;
            let responses = client.analyze_stream(stream_req).await?.into_inner();
            Ok(Self {
                submit: Some(AnalyzeSubmit {
                    requests: AnalyzeRequests::Remote { tx, binding },
                }),
                responses: AnalyzeResponses::Remote(Box::new(responses)),
                vocab,
                spec: spec.cloned(),
                layers,
            })
        }
    }

    fn open_native(
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
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
                "native lexical analysis does not provide {} layers; configure --analysis-addr=turbo or turbo-kserve for this ingest",
                requested.join(", ")
            )));
        }
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let dual_cased = layers.dual_cased;
        let parsed_spec = std::sync::Arc::new(native_spec(spec)?);
        let (requests, mut feed) = tokio::sync::mpsc::channel::<InProcessRequest>(SUBMIT_BUFFER);
        let (emit, responses) = tokio::sync::mpsc::channel::<InProcessResponse>(SUBMIT_BUFFER);
        tokio::spawn(async move {
            while let Some(request) = feed.recv().await {
                let sequence = request.sequence;
                let spec_clone = parsed_spec.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let native = if dual_cased {
                        native_dual_with_spec(&request.text, &spec_clone)?
                    } else {
                        native_analysis_with_spec(&request.text, &spec_clone)?
                    };
                    Ok(InProcessItem {
                        doc: native.doc,
                        surface_tokens: native.tokens,
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    Err(Status::internal(format!(
                        "native analysis worker failed: {error}"
                    )))
                });
                if emit
                    .send(InProcessResponse { sequence, result })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::InProcess(requests),
            }),
            responses: AnalyzeResponses::InProcess(responses),
            vocab,
            spec: spec.cloned(),
            layers,
        })
    }

    fn open_turbo(
        spec: Option<&AnalysisSpec>,
        vocab: Option<std::sync::Arc<crate::vocab::VocabularyListener>>,
        layers: SessionLayers,
    ) -> Result<Self, Status> {
        if layers.dual_cased {
            validate_dual_cased_spec(spec)?;
        }
        let _ = turbo_field_config(spec)?;
        if layers.quality || layers.geography || layers.entities {
            let _ = shared_turbo_nlp()?;
        }
        let owned_spec = spec.cloned();
        let (requests, mut feed) = tokio::sync::mpsc::channel::<InProcessRequest>(SUBMIT_BUFFER);
        let (emit, responses) = tokio::sync::mpsc::channel::<InProcessResponse>(SUBMIT_BUFFER);
        tokio::spawn(async move {
            while let Some(request) = feed.recv().await {
                let sequence = request.sequence;
                let spec_clone = owned_spec.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let out = turbo_analysis_pass(&request.text, spec_clone.as_ref(), layers)?;
                    Ok(InProcessItem {
                        doc: out.doc,
                        surface_tokens: out.surface_tokens,
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    Err(Status::internal(format!(
                        "turbo in-process analysis worker failed: {error}"
                    )))
                });
                if emit
                    .send(InProcessResponse { sequence, result })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Self {
            submit: Some(AnalyzeSubmit {
                requests: AnalyzeRequests::InProcess(requests),
            }),
            responses: AnalyzeResponses::InProcess(responses),
            vocab,
            spec: spec.cloned(),
            layers,
        })
    }

    /// Returns a cloneable submission handle.
    pub fn submitter(&self) -> AnalyzeSubmit {
        self.submit
            .clone()
            .expect("finished stream has no submitter")
    }

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
            AnalyzeResponses::Remote(responses) => match responses.message().await? {
                Some(response) => {
                    let sequence = response.id.parse::<u64>().map_err(|_| {
                        Status::internal(format!(
                            "analysis stream returned non-numeric correlation id {:?}",
                            response.id
                        ))
                    })?;
                    let result = match response.outcome {
                        Some(analyze_stream_response::Outcome::DocumentGraph(graph)) => {
                            decode_graph_response(
                                graph,
                                self.spec.as_ref(),
                                self.layers,
                                self.vocab.as_ref(),
                            )
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
                None => Ok(None),
            },
            AnalyzeResponses::InProcess(responses) => {
                let _ = &self.spec;
                let _ = &self.layers;
                match responses.recv().await {
                    Some(response) => {
                        let result = response.result.map(|item| {
                            if let Some(vocab) = &self.vocab {
                                let body = &item.doc.fields[0];
                                vocab.feed(
                                    body.terms.iter().map(|(term, frequency, _)| {
                                        (term.as_str(), i64::from(*frequency))
                                    }),
                                    item.surface_tokens.iter().map(String::as_str),
                                );
                            }
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

/// Analyze a batch through [`AnalyzeStream`], returning results in input order.
pub async fn analyze_batch(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>, SessionLayers)],
) -> Result<Vec<AnalyzedDoc>, Status> {
    analyze_batch_streams(addr, docs, 1).await
}

type SessionGroup<'a> = ((Option<&'a AnalysisSpec>, SessionLayers), Vec<usize>);

/// [`analyze_batch`] striped across `streams` concurrent [`AnalyzeStream`]s per `(spec, layers)`.
pub async fn analyze_batch_streams(
    addr: &str,
    docs: &[(&str, Option<&AnalysisSpec>, SessionLayers)],
    streams: usize,
) -> Result<Vec<AnalyzedDoc>, Status> {
    let mut out: Vec<Option<AnalyzedDoc>> = Vec::new();
    out.resize_with(docs.len(), || None);
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

async fn open_stream(
    addr: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzeStream, Status> {
    match AnalyzeStream::open_with_vocab(addr, spec, None, layers).await {
        Ok(session) => Ok(session),
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            Err(Status::failed_precondition(format!(
                "analysis service at {addr} does not implement AnalyzeStream; \
                 upgrade turbo-kserve to a build with AnalyzeStream support"
            )))
        }
        Err(status) => Err(status),
    }
}

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
    use super::wire::{
        analyzed_from_mock, doc_geography, doc_quality, MockAnalyzeResponse, WireGeoLocation,
        WireNoiseSpan, WireRegionVote, WireSpan, WireTermVector, WIRE_OFFSET_UTF16,
    };
    use super::*;

    fn noise_span(start: i32, end: i32, score: f64) -> WireNoiseSpan {
        WireNoiseSpan {
            span: Some(WireSpan { start, end }),
            severity: "gibberish".to_string(),
            score,
        }
    }

    #[test]
    fn noise_chars_is_the_union_not_the_sum() {
        let response = MockAnalyzeResponse {
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

    #[test]
    fn non_finite_noise_scores_are_ignored() {
        let response = MockAnalyzeResponse {
            noise: vec![noise_span(0, 4, f64::NAN), noise_span(4, 8, 0.5)],
            ..Default::default()
        };
        let q = doc_quality(&response);
        assert_eq!(q.noise, 0.5);
        assert_eq!(q.noise_chars, 8, "the NaN finding's SPAN still counts");
    }

    fn location(start: i32, confidence: f64, country: &str) -> WireGeoLocation {
        WireGeoLocation {
            span: Some(WireSpan {
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

    #[test]
    fn geography_picks_best_confidence_first_on_ties() {
        let response = MockAnalyzeResponse {
            locations: vec![
                location(0, 0.9, "FR"),
                location(10, 0.9, "DE"),
                location(20, 0.4, "US"),
            ],
            regions: vec![
                WireRegionVote {
                    country_code: "DE".to_string(),
                    share: 0.6,
                },
                WireRegionVote {
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

    #[test]
    fn non_finite_confidence_cannot_win() {
        let response = MockAnalyzeResponse {
            locations: vec![location(0, f64::NAN, "FR"), location(10, 0.3, "DE")],
            ..Default::default()
        };
        assert_eq!(doc_geography(&response).point, Some((10.0, -10.0)));

        let only_nan = MockAnalyzeResponse {
            locations: vec![location(0, f64::NAN, "FR")],
            ..Default::default()
        };
        assert_eq!(doc_geography(&only_nan).point, None);
    }

    #[test]
    fn no_locations_reduce_to_absence() {
        let g = doc_geography(&MockAnalyzeResponse::default());
        assert_eq!(g.point, None);
        assert_eq!(g.country, "");
    }

    #[test]
    fn a_clean_response_measures_zero() {
        let q = doc_quality(&MockAnalyzeResponse::default());
        assert_eq!(q.noise, 0.0);
        assert_eq!(q.noise_chars, 0);
        assert_eq!(q.artifacts, 0);
    }

    #[test]
    fn folded_and_cased_are_actually_different_analyzers() {
        let folded = analyzer_by_name("folded").expect("known name").unwrap();
        let cased = analyzer_by_name("cased").expect("known name").unwrap();
        assert_ne!(folded.term_vector_source, cased.term_vector_source);
        assert_eq!(folded.term_vector_source, SOURCE_NORMALIZED_STEMS);
        assert_eq!(cased.term_vector_source, SOURCE_STEMS);
        assert_eq!(folded.tokenizer, cased.tokenizer);
        assert_eq!(folded.stemmer, cased.stemmer);
        assert!(cased.char_filters.is_empty());
        assert!(folded.char_filters.contains(&CHAR_FILTER_FULL_CASE_FOLD));
    }

    #[test]
    fn ingest_is_the_corpus_analyzer() {
        assert_eq!(analyzer_by_name("ingest").unwrap(), Some(body_spec()));
        assert_eq!(analyzer_by_name("folded").unwrap(), Some(body_spec()));
    }

    #[test]
    fn server_means_no_spec() {
        assert_eq!(analyzer_by_name("server").unwrap(), None);
    }

    #[test]
    fn an_unknown_analyzer_is_refused_and_lists_the_known_ones() {
        let err = analyzer_by_name("porter").expect_err("not a known name");
        assert!(err.contains("porter"), "{err}");
        for name in ANALYZER_NAMES {
            assert!(err.contains(name), "error should list {name}: {err}");
        }
    }

    #[test]
    fn the_fingerprint_separates_the_analyzers_it_exists_to_separate() {
        let folded = analysis_fingerprint(Some(&body_spec()));
        let cased = analysis_fingerprint(Some(&cased_body_spec()));
        assert_ne!(folded, cased);
        assert_ne!(folded, 0);
        assert_ne!(cased, 0);
        assert_eq!(folded, analysis_fingerprint(Some(&body_spec())));
    }

    #[test]
    fn an_absent_spec_is_unknown_not_a_default() {
        assert_eq!(analysis_fingerprint(None), 0);
    }

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

    #[test]
    fn the_corpus_fingerprint_is_pinned() {
        assert_eq!(
            analysis_fingerprint(Some(&body_spec())),
            0x55eb_d3a6_febd_2ac3,
            "the corpus analyzer fingerprint changed"
        );
    }

    #[test]
    fn the_corpus_analyzer_folds_accents_and_skips_the_pdf_steps() {
        let filters = body_spec().char_filters;
        assert!(filters.contains(&CHAR_FILTER_ACCENT_FOLD));
        assert!(!filters.contains(&CHAR_FILTER_DEHYPHENATE));
        assert!(!filters.contains(&CHAR_FILTER_NFKC));
        assert!(cased_body_spec().char_filters.is_empty());
        assert_ne!(
            analysis_fingerprint(Some(&body_spec())),
            analysis_fingerprint(Some(&cased_body_spec())),
        );
    }

    #[test]
    fn zero_length_terms_are_dropped_at_the_boundary() {
        let tv = |term: &str, tf: i32| WireTermVector {
            term: term.to_string(),
            frequency: tf,
            occurrences: vec![WireSpan { start: 0, end: 5 }],
        };
        let with_empty = MockAnalyzeResponse {
            term_vectors: vec![tv("court", 2), tv("", 3), tv("appeal", 1)],
            ..Default::default()
        };
        let without = MockAnalyzeResponse {
            term_vectors: vec![tv("court", 2), tv("appeal", 1)],
            ..Default::default()
        };
        assert_eq!(
            analyzed_from_mock(with_empty, SessionLayers::default()).unwrap(),
            analyzed_from_mock(without, SessionLayers::default()).unwrap(),
        );
    }

    #[test]
    fn search_refuses_a_sidecar_that_violates_its_utf8_storage_request() {
        let response = MockAnalyzeResponse {
            offset_unit: WIRE_OFFSET_UTF16,
            ..Default::default()
        };
        let error = analyzed_from_mock(response, SessionLayers::default()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("ambiguous persisted spans"));
    }

    #[test]
    fn native_provider_runs_the_product_specs_with_utf8_offsets() {
        let folded =
            analyze_document_native("😀 Running Rodríguez running", Some(&body_spec())).unwrap();
        let body = &folded.fields[0];
        assert_eq!(body.length, 4);
        assert_eq!(
            body.terms,
            vec![
                ("😀".to_string(), 1, vec![(0, 4)]),
                ("run".to_string(), 2, vec![(5, 12), (24, 31)]),
                ("rodriguez".to_string(), 1, vec![(13, 23)]),
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
    fn turbo_inprocess_provider_projects_dehyphenated_utf8_offsets_and_nlp_layers() {
        let mut pdf_spec = body_spec();
        pdf_spec.char_filters.insert(0, CHAR_FILTER_DEHYPHENATE);
        let text = "The re-\nentered order in Paris cost $120.00.";
        let doc = analyze_document_turbo(
            text,
            Some(&pdf_spec),
            SessionLayers {
                quality: true,
                geography: true,
                entities: true,
                sentences: true,
                dual_cased: true,
                nlp_columns: false,
            },
        )
        .unwrap();
        let body = &doc.fields[0];
        body.check_positions().expect("valid ascending positions");
        body.check_sentences()
            .expect("sentences cover every occurrence");
        let reenter = body
            .terms
            .iter()
            .find(|(t, _, _)| t == "reenter")
            .expect("dehyphenated joined term present");
        assert_eq!(
            &text[reenter.2[0].0 as usize..reenter.2[0].1 as usize],
            "re-\nentered"
        );
        assert!(doc.cased.is_some(), "dual_cased twin populated in one pass");
        assert!(doc.quality.is_some(), "quality layer populated in-process");
        assert_eq!(
            doc.geography.as_ref().map(|g| g.country.as_str()),
            Some("FR"),
            "BundledGazetteer resolves Paris -> FR in-process"
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
                nlp_columns: false,
            },
        )
        .await
        .err()
        .expect("quality is not served by minimal native backend");
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
