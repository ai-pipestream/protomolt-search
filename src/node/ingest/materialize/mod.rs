//! CEL column materialization, placement verification, and phrase/proximity extraction.

use crate::node::*;

mod columns;
mod phrases;
mod proximity;

pub(crate) use self::columns::{apply_materialize, compile_materialize_spec};

/// One compiled MaterializeSpec, cached against spec equality.
pub(in crate::node) struct CompiledMaterialize {
    /// The spec these columns were compiled from, for the equality test.
    pub(in crate::node) spec: crate::pb::MaterializeSpec,
    /// (column name, compiled expression, target kind), in spec order.
    pub(in crate::node) columns: Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>,
}

/// Assemble one document's positional [`crate::postings::AnalyzedDoc`]:
/// body at field 0, extras at their table indexes, gaps empty. Body-only
/// documents pass through untouched (the exact pre-multi-field shape).
///
/// Nothing is awaited here. The apply wavefront only reaches a document
/// once every one of its fields has already landed, which is what keeps
/// a sidecar that holds a response (the test mock deliberately does, and
/// the streaming contract permits it) from stalling the whole ingest on
/// one field.
pub(crate) fn join_fields(
    mut body: crate::postings::AnalyzedDoc,
    extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
    cased: Option<usize>,
) -> Result<crate::postings::AnalyzedDoc, Status> {
    // Route the body's cased identity from the same analysis pass into `cased`.
    let cased_identity = body.cased.take();
    match (cased, &cased_identity) {
        (Some(_), None) => {
            return Err(Status::internal(
                "the document names a cased field but its analysis carried no cased identity",
            ))
        }
        (None, Some(_)) => {
            return Err(Status::internal(
                "the analysis carried a cased identity no field asked for",
            ))
        }
        _ => {}
    }
    if extras.is_empty() && cased.is_none() {
        return Ok(body);
    }
    let n = extras
        .iter()
        .map(|&(fi, _)| fi + 1)
        .chain(cased.map(|ci| ci + 1))
        .max()
        .unwrap_or(1);
    let mut fields = vec![crate::postings::AnalyzedField::default(); n];
    let quality = body.quality;
    let geography = body.geography.clone();
    let entities = body.entities.clone();
    let nlp = body.nlp.take();
    fields[0] = body.into_body();
    for (fi, analyzed) in extras {
        fields[fi] = analyzed.ok_or_else(|| {
            Status::internal(format!(
                "field {fi} applied before its analysis arrived; the apply \
                 wavefront must not advance past an unfilled field"
            ))
        })?;
    }
    if let (Some(ci), Some(identity)) = (cased, cased_identity) {
        fields[ci] = identity;
    }
    Ok(crate::postings::AnalyzedDoc {
        fields,
        cased: None,
        quality,
        geography,
        entities,
        nlp,
    })
}

/// Validate `cased_field` as a declared non-body, non-derived BM25 field paired
/// with a dual-cased step-chain body spec, returning `None` when unset.
pub(crate) fn cased_field_index(
    config: &NodeConfig,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    doc: &AddDocumentsRequest,
) -> Result<Option<usize>, Status> {
    if doc.cased_field.is_empty() {
        return Ok(None);
    }
    let name = doc.cased_field.as_str();
    if name == "body" {
        return Err(Status::invalid_argument(
            "cased_field must name a field other than \"body\": the body is the folded identity",
        ));
    }
    if phrase_index.is_some_and(|phrases| name == phrases.phrase_field()) {
        return Err(Status::invalid_argument(format!(
            "cased_field {name:?} is the configured phrase glossary field; it is derived, not \
             cased"
        )));
    }
    if let Some(source) = config
        .bigram_fields
        .iter()
        .find(|source| crate::proximity::bigram_field_name(source) == name)
    {
        return Err(Status::invalid_argument(format!(
            "cased_field {name:?} is the bigram column derived from {source:?}; it is derived, \
             not cased"
        )));
    }
    let Some(fi) = config.bm25_fields.iter().position(|n| n == name) else {
        return Err(Status::invalid_argument(format!(
            "unknown cased_field {name:?}; this shard indexes {:?}",
            config.bm25_fields
        )));
    };
    crate::analyzer::validate_dual_cased_spec(doc.analysis.as_ref())?;
    Ok(Some(fi))
}

/// Whether an ingest asked for any quality column at all. An empty
/// spec (every column name blank) asks for nothing, and must not make
/// the node request layers the caller will not store.
pub(in crate::node) fn quality_wanted(spec: Option<&crate::pb::QualitySpec>) -> bool {
    spec.is_some_and(|q| {
        !q.noise_column.is_empty()
            || !q.noise_chars_column.is_empty()
            || !q.artifact_column.is_empty()
    })
}

/// Whether an ingest asked for any geography column at all. Same
/// blank-spec rule as [`quality_wanted`]: a spec with every column
/// name empty asks for nothing and must not make the session request
/// (and pay for) the geocoding layer.
pub(in crate::node) fn geography_wanted(spec: Option<&crate::pb::GeographySpec>) -> bool {
    spec.is_some_and(|g| {
        !g.point_column.is_empty()
            || !g.country_column.is_empty()
            || !g.confidence_column.is_empty()
    })
}

/// Canonical content hash of a materialize spec for mapped-plan bindings
/// (empty when unset or when `columns` is empty).
pub(crate) fn materialize_sha(spec: Option<&crate::pb::MaterializeSpec>) -> String {
    let Some(spec) = spec else {
        return String::new();
    };
    if spec.columns.is_empty() {
        return String::new();
    }
    let mut hasher = turbo_sha256::Sha256::new();
    // An I64 column changes the hash domain: a binding recorded without I64
    // presence semantics could discard a valid MIN result, so appends to it
    // refuse instead of mixing projection semantics. F64-only specs keep the
    // untagged hash.
    if spec
        .columns
        .iter()
        .any(|column| column.kind == crate::pb::MaterializeKind::I64 as i32)
    {
        hasher.update(b"pipestream-search.materialize.i64-presence.v1\0");
    }
    for column in &spec.columns {
        for part in [column.name.as_str(), column.expression.as_str()] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        hasher.update(&column.kind.to_be_bytes());
    }
    hasher.finish().to_hex()
}

/// Fold derived quality scalars into `doc.numerics` / `doc.integers` and clear
/// `doc.quality`, failing if requested quality layers were omitted by analysis.
fn materialize_quality(
    mut doc: AddDocumentsRequest,
    analyzed: &crate::postings::AnalyzedDoc,
) -> Result<AddDocumentsRequest, Status> {
    let Some(spec) = doc.quality.take() else {
        return Ok(doc);
    };
    if !quality_wanted(Some(&spec)) {
        return Ok(doc);
    }
    let Some(quality) = analyzed.quality else {
        return Err(Status::internal(
            "quality columns were requested but the analysis session returned no quality layers; \
             the sidecar's options and its responses disagree",
        ));
    };
    if !spec.noise_column.is_empty() {
        doc.numerics.push(crate::pb::NumericValue {
            field: spec.noise_column,
            value: quality.noise,
        });
    }
    if !spec.noise_chars_column.is_empty() {
        doc.integers.push(crate::pb::IntegerValue {
            field: spec.noise_chars_column,
            value: quality.noise_chars,
        });
    }
    if !spec.artifact_column.is_empty() {
        doc.integers.push(crate::pb::IntegerValue {
            field: spec.artifact_column,
            value: quality.artifacts,
        });
    }
    Ok(doc)
}

/// Fold geocoding results into `doc.geo_points` / `doc.facets` / `doc.numerics`
/// and clear `doc.geography`, omitting unresolvable coordinates or countries.
fn materialize_geography(
    mut doc: AddDocumentsRequest,
    analyzed: &crate::postings::AnalyzedDoc,
) -> Result<AddDocumentsRequest, Status> {
    let Some(spec) = doc.geography.take() else {
        return Ok(doc);
    };
    if !geography_wanted(Some(&spec)) {
        return Ok(doc);
    }
    let Some(geography) = analyzed.geography.as_ref() else {
        return Err(Status::internal(
            "geography columns were requested but the analysis session returned no \
             geocoding layer; the sidecar's options and its responses disagree",
        ));
    };
    if let Some((lat, lon)) = geography.point {
        if !spec.point_column.is_empty() {
            doc.geo_points.push(crate::pb::GeoPointValue {
                field: spec.point_column,
                lat,
                lon,
            });
        }
        if !spec.confidence_column.is_empty() {
            doc.numerics.push(crate::pb::NumericValue {
                field: spec.confidence_column,
                value: geography.confidence,
            });
        }
    }
    if !geography.country.is_empty() && !spec.country_column.is_empty() {
        doc.facets.push(crate::pb::FacetValue {
            field: spec.country_column,
            value: geography.country.clone(),
        });
    }
    Ok(doc)
}

impl NodeServiceImpl {
    /// The pre-lock half of [`Self::apply_analyzed_document`]: derive or
    /// validate the document's proximity, phrase, quality, geography, and
    /// materialized columns. On a document that already went through it
    /// (a WAL record) every step validates instead of deriving, so the
    /// compaction shadow replays records through the same function.
    pub(crate) fn materialize_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        stable_key: Option<&[u8]>,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        if let Some(hs) = self.turbo_handshake.as_deref() {
            let cfg = crate::analyzer::turbo_field_config(doc.analysis.as_ref())?;
            hs.verify_analysis(&cfg.fingerprint()).map_err(|e| {
                Status::failed_precondition(format!("analyzer fingerprint handshake mismatch: {e}"))
            })?;
        } else if doc.analysis.is_some() {
            let cfg = crate::analyzer::turbo_field_config(doc.analysis.as_ref())?;
            let fp = cfg.fingerprint();
            let mut auto = self
                .auto_turbo_handshake
                .lock()
                .expect("auto_turbo_handshake lock poisoned");
            match auto.as_ref() {
                Some(pinned) => {
                    pinned.verify_analysis(&fp).map_err(|e| {
                        Status::failed_precondition(format!(
                            "analyzer fingerprint handshake mismatch: {e}"
                        ))
                    })?;
                }
                None => {
                    *auto = Some(turbo_client::IndexGenerationHandshake {
                        analysis: Some(fp),
                        bundle: None,
                    });
                }
            }
        }
        let (doc, analyzed) = self.materialize_proximity(doc, analyzed)?;
        let (doc, analyzed) = self.materialize_phrases(doc, analyzed)?;
        let doc = materialize_quality(doc, &analyzed)?;
        let doc = materialize_geography(doc, &analyzed)?;
        let doc = crate::turbo_bridge::materialize_nlp_columns(
            doc,
            analyzed.nlp.as_deref(),
            self.nlp_columns.as_deref(),
        )?;
        let doc = self.materialize_columns(doc, stable_key)?;
        let doc = self.place_document(doc)?;
        Ok((doc, analyzed))
    }
}
