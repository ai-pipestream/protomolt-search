//! Canonical wire session binding and mock graph codec bridging
//! `protomolt-search` analysis sessions with `turbo.result.v1.AnalysisService`.

#[cfg(any(feature = "net", test))]
use tonic::Status;

#[cfg(any(feature = "net", test))]
use crate::analyzer::{SessionLayers, EMPTY_TERMS_DROPPED};
#[cfg(any(feature = "net", test))]
use crate::pb::AnalysisSpec;
#[cfg(any(feature = "net", test))]
use crate::postings::{
    AnalyzedDoc, AnalyzedField, DocGeography, DocPositions, DocQuality, DocTerms,
};

/// `InputBinding.parse_artifact_id` carrying serialized [`WireSessionOptions`].
#[cfg(any(feature = "net", test))]
pub(crate) const SESSION_BINDING_ID: &str = "protomolt.session.v1";

/// Binary magic identifying a deterministic `MockAnalysis` payload inside
/// `DocumentGraphResponse.graph` (distinct from `b"TGRF"` binary graphs).
#[cfg(any(feature = "net", test))]
pub(crate) const MOCK_GRAPH_MAGIC: &[u8; 5] = b"TMCK\x01";

/// Profile marker advertised only by `start_mock_analysis_without_dual_identity`
/// to simulate a server lacking dual cased/folded term extraction.
#[cfg(any(feature = "net", test))]
pub(crate) const NO_DUAL_CASED_PROFILE: &str = "protomolt:no-dual-cased";

/// Wire offset unit codes preserved for UTF-8 storage invariant checks.
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UNSPECIFIED: i32 = 0;
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UTF8: i32 = 1;
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UTF16: i32 = 2;

/// Session-level lexical specification and optional layer flags carried in
/// `InputBinding` on `AnalyzeStream` and `AnalyzeDocumentProgressive`.
#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireSessionOptions {
    pub(crate) spec: Option<AnalysisSpec>,
    pub(crate) layers: SessionLayers,
}

#[cfg(any(feature = "net", test))]
impl WireSessionOptions {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.push(1u8);
        let mut layer_bits = 0u8;
        if self.layers.quality {
            layer_bits |= 1 << 0;
        }
        if self.layers.geography {
            layer_bits |= 1 << 1;
        }
        if self.layers.entities {
            layer_bits |= 1 << 2;
        }
        if self.layers.sentences {
            layer_bits |= 1 << 3;
        }
        if self.layers.dual_cased {
            layer_bits |= 1 << 4;
        }
        if self.layers.nlp_columns {
            layer_bits |= 1 << 5;
        }
        out.push(layer_bits);
        match &self.spec {
            None => out.push(0u8),
            Some(s) => {
                out.push(1u8);
                put_i32(&mut out, s.tokenizer);
                put_i32(&mut out, s.stemmer);
                put_i32(&mut out, s.term_vector_mode);
                put_i32(&mut out, s.term_vector_source);
                put_u32(&mut out, s.char_filters.len() as u32);
                for &f in &s.char_filters {
                    put_i32(&mut out, f);
                }
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let mut r = Reader::new(bytes);
        let ver = r.u8()?;
        if ver != 1 {
            return Err(Status::invalid_argument(format!(
                "unsupported wire session options version {ver}"
            )));
        }
        let layer_bits = r.u8()?;
        let layers = SessionLayers {
            quality: (layer_bits & (1 << 0)) != 0,
            geography: (layer_bits & (1 << 1)) != 0,
            entities: (layer_bits & (1 << 2)) != 0,
            sentences: (layer_bits & (1 << 3)) != 0,
            dual_cased: (layer_bits & (1 << 4)) != 0,
            nlp_columns: (layer_bits & (1 << 5)) != 0,
        };
        let has_spec = r.u8()?;
        let spec = if has_spec == 0 {
            None
        } else {
            let tokenizer = r.i32()?;
            let stemmer = r.i32()?;
            let term_vector_mode = r.i32()?;
            let term_vector_source = r.i32()?;
            let n = r.u32()? as usize;
            if n > 1024 {
                return Err(Status::invalid_argument("excessive char_filters length"));
            }
            let mut char_filters = Vec::with_capacity(n);
            for _ in 0..n {
                char_filters.push(r.i32()?);
            }
            Some(AnalysisSpec {
                tokenizer,
                stemmer,
                term_vector_mode,
                term_vector_source,
                char_filters,
            })
        };
        Ok(Self { spec, layers })
    }

    #[cfg(feature = "net")]
    pub(crate) fn to_binding(&self) -> turbo_client::proto::result::InputBinding {
        turbo_client::proto::result::InputBinding {
            source_revision: String::new(),
            parse_artifact_id: SESSION_BINDING_ID.to_string(),
            parse_artifact_digest: self.encode(),
        }
    }

    #[cfg(feature = "net")]
    pub(crate) fn from_binding(
        binding: Option<&turbo_client::proto::result::InputBinding>,
    ) -> Result<Self, Status> {
        let Some(b) = binding else {
            return Ok(Self::default());
        };
        if b.parse_artifact_id != SESSION_BINDING_ID || b.parse_artifact_digest.is_empty() {
            return Ok(Self::default());
        }
        Self::decode(&b.parse_artifact_digest)
    }
}

#[cfg(any(feature = "net", test))]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireSpan {
    pub(crate) start: i32,
    pub(crate) end: i32,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireToken {
    pub(crate) span: Option<WireSpan>,
    pub(crate) text: String,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireTermVector {
    pub(crate) term: String,
    pub(crate) frequency: i32,
    pub(crate) occurrences: Vec<WireSpan>,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireNoiseSpan {
    pub(crate) span: Option<WireSpan>,
    pub(crate) severity: String,
    pub(crate) score: f64,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireTextArtifact {
    pub(crate) span: Option<WireSpan>,
    pub(crate) kind: String,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireGeoLocation {
    pub(crate) span: Option<WireSpan>,
    pub(crate) name: String,
    pub(crate) country_code: String,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
    pub(crate) confidence: f64,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireRegionVote {
    pub(crate) country_code: String,
    pub(crate) share: f64,
}

#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireEntity {
    pub(crate) span: Option<WireSpan>,
    pub(crate) kind: String,
    pub(crate) text: String,
}

/// Deterministic analysis payload used by `MockAnalysis` and unit tests.
#[cfg(any(feature = "net", test))]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MockAnalyzeResponse {
    pub(crate) offset_unit: i32,
    pub(crate) sentences: Vec<WireSpan>,
    pub(crate) tokens: Vec<WireToken>,
    pub(crate) term_vectors: Vec<WireTermVector>,
    pub(crate) cased_term_vectors: Vec<WireTermVector>,
    pub(crate) noise: Vec<WireNoiseSpan>,
    pub(crate) artifacts: Vec<WireTextArtifact>,
    pub(crate) locations: Vec<WireGeoLocation>,
    pub(crate) regions: Vec<WireRegionVote>,
    pub(crate) entities: Vec<WireEntity>,
}

#[cfg(any(feature = "net", test))]
impl MockAnalyzeResponse {
    pub(crate) fn encode_graph(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(MOCK_GRAPH_MAGIC);
        put_i32(&mut out, self.offset_unit);
        put_u32(&mut out, self.sentences.len() as u32);
        for s in &self.sentences {
            put_span(&mut out, Some(*s));
        }
        put_u32(&mut out, self.tokens.len() as u32);
        for t in &self.tokens {
            put_span(&mut out, t.span);
            put_str(&mut out, &t.text);
        }
        encode_term_vectors(&mut out, &self.term_vectors);
        encode_term_vectors(&mut out, &self.cased_term_vectors);
        put_u32(&mut out, self.noise.len() as u32);
        for n in &self.noise {
            put_span(&mut out, n.span);
            put_str(&mut out, &n.severity);
            put_f64(&mut out, n.score);
        }
        put_u32(&mut out, self.artifacts.len() as u32);
        for a in &self.artifacts {
            put_span(&mut out, a.span);
            put_str(&mut out, &a.kind);
        }
        put_u32(&mut out, self.locations.len() as u32);
        for loc in &self.locations {
            put_span(&mut out, loc.span);
            put_str(&mut out, &loc.name);
            put_str(&mut out, &loc.country_code);
            put_f64(&mut out, loc.latitude);
            put_f64(&mut out, loc.longitude);
            put_f64(&mut out, loc.confidence);
        }
        put_u32(&mut out, self.regions.len() as u32);
        for r in &self.regions {
            put_str(&mut out, &r.country_code);
            put_f64(&mut out, r.share);
        }
        put_u32(&mut out, self.entities.len() as u32);
        for e in &self.entities {
            put_span(&mut out, e.span);
            put_str(&mut out, &e.kind);
            put_str(&mut out, &e.text);
        }
        out
    }

    pub(crate) fn decode_graph(bytes: &[u8]) -> Result<Self, Status> {
        let Some(body) = bytes.strip_prefix(MOCK_GRAPH_MAGIC) else {
            return Err(Status::internal("invalid mock graph magic"));
        };
        let mut r = Reader::new(body);
        let offset_unit = r.i32()?;
        let n_sent = r.u32()? as usize;
        let mut sentences = Vec::with_capacity(n_sent);
        for _ in 0..n_sent {
            if let Some(s) = r.span()? {
                sentences.push(s);
            }
        }
        let n_tok = r.u32()? as usize;
        let mut tokens = Vec::with_capacity(n_tok);
        for _ in 0..n_tok {
            let span = r.span()?;
            let text = r.string()?;
            tokens.push(WireToken { span, text });
        }
        let term_vectors = decode_term_vectors(&mut r)?;
        let cased_term_vectors = decode_term_vectors(&mut r)?;
        let n_noise = r.u32()? as usize;
        let mut noise = Vec::with_capacity(n_noise);
        for _ in 0..n_noise {
            let span = r.span()?;
            let severity = r.string()?;
            let score = r.f64()?;
            noise.push(WireNoiseSpan {
                span,
                severity,
                score,
            });
        }
        let n_art = r.u32()? as usize;
        let mut artifacts = Vec::with_capacity(n_art);
        for _ in 0..n_art {
            let span = r.span()?;
            let kind = r.string()?;
            artifacts.push(WireTextArtifact { span, kind });
        }
        let n_loc = r.u32()? as usize;
        let mut locations = Vec::with_capacity(n_loc);
        for _ in 0..n_loc {
            let span = r.span()?;
            let name = r.string()?;
            let country_code = r.string()?;
            let latitude = r.f64()?;
            let longitude = r.f64()?;
            let confidence = r.f64()?;
            locations.push(WireGeoLocation {
                span,
                name,
                country_code,
                latitude,
                longitude,
                confidence,
            });
        }
        let n_reg = r.u32()? as usize;
        let mut regions = Vec::with_capacity(n_reg);
        for _ in 0..n_reg {
            let country_code = r.string()?;
            let share = r.f64()?;
            regions.push(WireRegionVote {
                country_code,
                share,
            });
        }
        let n_ent = r.u32()? as usize;
        let mut entities = Vec::with_capacity(n_ent);
        for _ in 0..n_ent {
            let span = r.span()?;
            let kind = r.string()?;
            let text = r.string()?;
            entities.push(WireEntity { span, kind, text });
        }
        Ok(Self {
            offset_unit,
            sentences,
            tokens,
            term_vectors,
            cased_term_vectors,
            noise,
            artifacts,
            locations,
            regions,
            entities,
        })
    }
}

/// Converts a `turbo-kserve` `AnalyzeFailure` into a `tonic::Status`.
#[cfg(feature = "net")]
pub(crate) fn status_from_analyze_failure(
    failure: turbo_client::proto::result::AnalyzeFailure,
) -> Status {
    let code = match failure.turbo_code {
        1..=16 => tonic::Code::from(failure.turbo_code),
        256 | 258 | 260 | 262 => tonic::Code::InvalidArgument,
        261 | 1024 | 1281 | 1283 => tonic::Code::FailedPrecondition,
        512..=514 => tonic::Code::Unimplemented,
        768 => tonic::Code::ResourceExhausted,
        769 | 1025 => tonic::Code::Unavailable,
        771 => tonic::Code::OutOfRange,
        1280 => tonic::Code::NotFound,
        1282 => tonic::Code::DataLoss,
        _ => tonic::Code::Internal,
    };
    Status::new(code, failure.message)
}

/// Encodes a `tonic::Status` into a `turbo-kserve` `AnalyzeFailure`.
#[cfg(feature = "net")]
pub(crate) fn analyze_failure_from_status(
    status: Status,
) -> turbo_client::proto::result::AnalyzeFailure {
    turbo_client::proto::result::AnalyzeFailure {
        turbo_code: status.code() as i32,
        field: 0,
        message: status.message().to_string(),
    }
}

#[cfg(any(feature = "net", test))]
fn terms_of(vectors: Vec<WireTermVector>) -> (DocTerms, u32) {
    let mut terms = DocTerms::new();
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

#[cfg(any(feature = "net", test))]
pub(crate) fn analyzed_from_mock(
    mut response: MockAnalyzeResponse,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    match response.offset_unit {
        WIRE_OFFSET_UNSPECIFIED | WIRE_OFFSET_UTF8 => {}
        WIRE_OFFSET_UTF16 => {
            return Err(Status::failed_precondition(
                "analysis service returned UTF-16 offsets after search requested UTF-8; refusing ambiguous persisted spans",
            ));
        }
        value => {
            return Err(Status::failed_precondition(format!(
                "analysis service returned unknown offset unit {value}"
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
                    kind: entity.kind.clone(),
                    text: entity.text.clone(),
                    start: span.map_or(0, |s| s.start.max(0) as u32),
                    end: span.map_or(0, |s| s.end.max(0) as u32),
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let (terms, length) = terms_of(std::mem::take(&mut response.term_vectors));
    let positions = token_positions(&response.tokens, &terms)?;
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
        let (cased_terms, cased_length) =
            terms_of(std::mem::take(&mut response.cased_term_vectors));
        if cased_terms.is_empty() && !doc.fields[0].terms.is_empty() {
            return Err(Status::failed_precondition(
                "dual_cased was requested but the analysis service returned no cased term \
                 identities; upgrade turbo-kserve or disable cased_field",
            ));
        }
        let cased_positions = token_positions(&response.tokens, &cased_terms)?;
        doc.cased = Some(AnalyzedField {
            terms: cased_terms,
            length: cased_length,
            positions: cased_positions,
            sentences,
            ..Default::default()
        });
    }
    doc.quality = quality;
    doc.geography = geography;
    doc.entities = entities;
    Ok(doc)
}

#[cfg(any(feature = "net", test))]
fn token_positions(tokens: &[WireToken], terms: &DocTerms) -> Result<Option<DocPositions>, Status> {
    let occurrences: usize = terms.iter().map(|(_, _, offsets)| offsets.len()).sum();
    if occurrences == 0 {
        return Ok(Some(terms.iter().map(|_| Vec::new()).collect()));
    }
    if tokens.is_empty() {
        return Ok(None);
    }
    let mut wanted: Vec<(u32, u32, usize, usize)> = Vec::with_capacity(occurrences);
    for (ti, (_, _, offsets)) in terms.iter().enumerate() {
        for (oi, &(start, end)) in offsets.iter().enumerate() {
            wanted.push((start, end, ti, oi));
        }
    }
    wanted.sort_unstable();
    let mut positions: DocPositions = terms
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
                "analysis service reported occurrence [{start}, {end}) of {:?} that is not a \
                 token span; token positions cannot be derived from this response",
                terms[ti].0
            )));
        }
        positions[ti][oi] = u32::try_from(cursor).expect("token count fits u32 under the text cap");
        cursor += 1;
    }
    Ok(Some(positions))
}

#[cfg(any(feature = "net", test))]
pub(crate) fn doc_quality(response: &MockAnalyzeResponse) -> DocQuality {
    let mut worst = 0.0f64;
    let mut spans: Vec<(i64, i64)> = Vec::with_capacity(response.noise.len());
    for finding in &response.noise {
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
    DocQuality {
        noise: worst,
        noise_chars: covered,
        artifacts: response.artifacts.len() as i64,
    }
}

#[cfg(any(feature = "net", test))]
pub(crate) fn doc_geography(response: &MockAnalyzeResponse) -> DocGeography {
    let mut best: Option<&WireGeoLocation> = None;
    for location in &response.locations {
        if !location.confidence.is_finite() {
            continue;
        }
        if best.is_none_or(|b| location.confidence > b.confidence) {
            best = Some(location);
        }
    }
    DocGeography {
        point: best.map(|b| (b.latitude, b.longitude)),
        confidence: best.map_or(0.0, |b| b.confidence),
        country: response
            .regions
            .first()
            .map(|r| r.country_code.clone())
            .unwrap_or_default(),
    }
}

#[cfg(any(feature = "net", test))]
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(any(feature = "net", test))]
fn put_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(any(feature = "net", test))]
fn put_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(any(feature = "net", test))]
fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

#[cfg(any(feature = "net", test))]
fn put_span(out: &mut Vec<u8>, span: Option<WireSpan>) {
    match span {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            put_i32(out, s.start);
            put_i32(out, s.end);
        }
    }
}

#[cfg(any(feature = "net", test))]
fn encode_term_vectors(out: &mut Vec<u8>, tvs: &[WireTermVector]) {
    put_u32(out, tvs.len() as u32);
    for tv in tvs {
        put_str(out, &tv.term);
        put_i32(out, tv.frequency);
        put_u32(out, tv.occurrences.len() as u32);
        for occ in &tv.occurrences {
            put_i32(out, occ.start);
            put_i32(out, occ.end);
        }
    }
}

#[cfg(any(feature = "net", test))]
fn decode_term_vectors(r: &mut Reader<'_>) -> Result<Vec<WireTermVector>, Status> {
    let n = r.u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let term = r.string()?;
        let frequency = r.i32()?;
        let n_occ = r.u32()? as usize;
        let mut occurrences = Vec::with_capacity(n_occ);
        for _ in 0..n_occ {
            let start = r.i32()?;
            let end = r.i32()?;
            occurrences.push(WireSpan { start, end });
        }
        out.push(WireTermVector {
            term,
            frequency,
            occurrences,
        });
    }
    Ok(out)
}

#[cfg(any(feature = "net", test))]
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[cfg(any(feature = "net", test))]
impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], Status> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Status::internal("wire decode overflow"))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Status::internal("truncated wire payload"))?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, Status> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, Status> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, Status> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, Status> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String, Status> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|e| Status::internal(format!("invalid UTF-8 in wire payload: {e}")))
    }

    fn span(&mut self) -> Result<Option<WireSpan>, Status> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let start = self.i32()?;
                let end = self.i32()?;
                Ok(Some(WireSpan { start, end }))
            }
            tag => Err(Status::internal(format!("invalid wire span tag {tag}"))),
        }
    }
}
