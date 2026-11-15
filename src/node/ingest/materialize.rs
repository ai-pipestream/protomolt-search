//! CEL column materialization, placement verification, and phrase/proximity extraction.

use crate::node::*;

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
    let mut hasher = crate::sha256::Sha256::new();
    // Earlier I64 materialization silently discarded a valid MIN result.
    // Refuse appends to those bindings instead of mixing projection semantics.
    // F64-only specs retain their unchanged interpretation and hash.
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
    crate::sha256::to_hex(&hasher.finalize())
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
    /// Compile (or fetch the cached compilation of) one ingest
    /// materialization spec. Validation is whole-spec: empty or
    /// duplicate names, an unset kind, or an expression that does not
    /// compile all refuse before any document is touched.
    pub(in crate::node) fn compiled_materialize(
        &self,
        spec: &crate::pb::MaterializeSpec,
    ) -> Result<Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>, Status> {
        let mut cache = self
            .materialize_cache
            .lock()
            .expect("materialize cache lock poisoned");
        if let Some(compiled) = cache.as_ref() {
            if compiled.spec == *spec {
                return Ok(compiled.columns.clone());
            }
        }
        let columns = compile_materialize_spec(spec)?;
        if let Some(declaration) = self.config.derived.as_deref() {
            for (name, _, _) in &columns {
                if declaration.is_derived(name) {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} is a derived column of this index's \
                         declaration, which the index computes itself; a request's spec \
                         cannot redefine it"
                    )));
                }
            }
        }
        let sources = source_tables(&self.config);
        let types = sources.types();
        for ((name, expr, kind), source) in columns.iter().zip(&spec.columns) {
            let (_, vt) = crate::values::resolve(expr, &types).map_err(|e| {
                Status::invalid_argument(format!("materialize: column {name:?}: {}", e.message()))
            })?;
            let expected = match kind {
                crate::pb::MaterializeKind::F64 => crate::values::ValueType::Double,
                crate::pb::MaterializeKind::I64 => crate::values::ValueType::Int,
                crate::pb::MaterializeKind::U64 => crate::values::ValueType::Uint,
                _ => unreachable!("compile_materialize_spec checked the kind"),
            };
            if vt != crate::values::ValueType::Unknown && vt != expected {
                let hint = if vt == crate::values::ValueType::Bool {
                    "wrap the boolean in a ternary that yields the target numeric type"
                } else {
                    "align the target kind with the expression; use double(...) for an explicit F64 conversion"
                };
                return Err(Status::invalid_argument(format!(
                    "materialize: column {name:?}, expression {:?}, declares {kind:?} but resolves to {vt:?}; {hint}", source.expression
                )));
            }
        }
        *cache = Some(CompiledMaterialize {
            spec: spec.clone(),
            columns: columns.clone(),
        });
        Ok(columns)
    }

    /// Verify pinned placement-leaf predicates against a live-ingest document
    /// (including value-dialect materialization and index-declared derived columns).
    pub(in crate::node) fn check_placement(
        &self,
        doc: &AddDocumentsRequest,
        stable_key: Option<&[u8]>,
    ) -> Result<(), Status> {
        let Some(pinned) = self.config.placement_tree.as_ref() else {
            return Ok(());
        };
        let mut routed = doc.clone();
        if let Some(spec) = doc
            .materialize
            .as_ref()
            .filter(|spec| !spec.columns.is_empty())
        {
            let compiled = self.compiled_materialize(spec)?;
            routed.materialize = None;
            routed = apply_materialize(routed, &compiled)?;
        }
        if let Some(declaration) = self.config.derived.as_deref() {
            if routed.derived_fingerprint.is_empty() {
                routed = declaration.apply(routed, stable_key)?;
            }
        }
        let outcome = pinned.check(&routed);
        outcome.map_err(|reason| {
            Status::invalid_argument(format!(
                "{reason}; this shard pins leaf {} (--placement-leaf) under the tree in \
                 --placement-tree, so route the document through the coordinator or send it \
                 to a shard of its leaf",
                pinned.leaf().code
            ))
        })
    }

    /// Enforce placement-column assignment before applying `doc`: populate from
    /// `placement_leaf` when absent, reject mismatched leaf codes, or require an
    /// explicit code when `placement_column` is declared without a pinned leaf.
    pub(in crate::node) fn place_document(
        &self,
        mut doc: AddDocumentsRequest,
    ) -> Result<AddDocumentsRequest, Status> {
        let Some(column) = self.config.placement_column.as_deref() else {
            return Ok(doc);
        };
        let carried = doc
            .integers
            .iter()
            .find(|value| value.field == column)
            .map(|value| value.value);
        match (self.config.placement_leaf, carried) {
            (Some(code), None) => {
                doc.integers.push(crate::pb::IntegerValue {
                    field: column.to_string(),
                    value: code,
                });
            }
            (Some(code), Some(other)) if other != code => {
                return Err(Status::invalid_argument(format!(
                    "placement column {column:?} carries code {other}, but this shard serves \
                     placement leaf {code} (--placement-leaf); route the document through the \
                     coordinator or send it to the shard that serves {other}"
                )));
            }
            (None, None) => {
                return Err(Status::invalid_argument(format!(
                    "placement column {column:?} has no value on this document and this shard \
                     pins no leaf (--placement-leaf); route the document through the \
                     coordinator, which evaluates the placement tree, or pin the leaf"
                )));
            }
            _ => {}
        }
        Ok(doc)
    }

    /// Evaluate request and index-declared derived columns into `doc`'s typed
    /// value lists, clearing `doc.materialize` so WAL replay never re-evaluates.
    pub(in crate::node) fn materialize_columns(
        &self,
        mut doc: AddDocumentsRequest,
        stable_key: Option<&[u8]>,
    ) -> Result<AddDocumentsRequest, Status> {
        if let Some(spec) = doc
            .materialize
            .take()
            .filter(|spec| !spec.columns.is_empty())
        {
            let compiled = self.compiled_materialize(&spec)?;
            doc = apply_materialize(doc, &compiled)?;
        }
        // Compute and stamp index-declared derived columns on fresh documents,
        // or verify the recorded fingerprint on replayed records.
        if let Some(declaration) = self.config.derived.as_deref() {
            if doc.derived_fingerprint.is_empty() {
                doc = declaration.apply(doc, stable_key)?;
            } else {
                declaration.check_logged(&doc)?;
            }
        } else if !doc.derived_fingerprint.is_empty() {
            return Err(Status::failed_precondition(
                "the document carries a derived fingerprint, but this index declares no derived columns",
            ));
        }
        Ok(doc)
    }
}

/// Validate and compile one materialize spec, refusing empty or duplicate names,
/// unset target kinds, or invalid CEL expressions before mutating state.
pub(crate) fn compile_materialize_spec(
    spec: &crate::pb::MaterializeSpec,
) -> Result<Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>, Status> {
    let mut names = std::collections::HashSet::new();
    let mut columns = Vec::with_capacity(spec.columns.len());
    for column in &spec.columns {
        if column.name.is_empty() {
            return Err(Status::invalid_argument(
                "materialize: a derived column needs a non-empty name",
            ));
        }
        if !names.insert(column.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "materialize: duplicate derived column {:?}",
                column.name
            )));
        }
        let kind = match crate::pb::MaterializeKind::try_from(column.kind) {
            Ok(crate::pb::MaterializeKind::F64) => crate::pb::MaterializeKind::F64,
            Ok(crate::pb::MaterializeKind::I64) => crate::pb::MaterializeKind::I64,
            Ok(crate::pb::MaterializeKind::U64) => crate::pb::MaterializeKind::U64,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "materialize: column {:?} declares no target kind; kinds are \
                     explicit (MATERIALIZE_KIND_F64, MATERIALIZE_KIND_I64, or MATERIALIZE_KIND_U64), \
                     never inferred from data",
                    column.name
                )))
            }
        };
        let expr = crate::cel::compile_value(&column.expression).map_err(|e| {
            Status::invalid_argument(format!(
                "materialize: column {:?}: {}",
                column.name,
                e.message()
            ))
        })?;
        columns.push((column.name.clone(), expr, kind));
    }
    Ok(columns)
}

/// Evaluate compiled materialized columns over one document's own
/// values and push the results into its `numerics` / `integers`
/// lists. Shared by node ingest and the coordinator's placement
/// evaluation, so a placement predicate on a derived column sees the
/// value the shard will store.
pub(crate) fn apply_materialize(
    doc: AddDocumentsRequest,
    compiled: &[(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)],
) -> Result<AddDocumentsRequest, Status> {
    let env = crate::derived::ingest_env(&doc, None)?;
    crate::derived::materialize_into(doc, compiled, &env, false)
}

impl NodeServiceImpl {
    /// Derive or validate phrase postings, materialize entity map entries on
    /// the first pass, and install the dedicated analyzed field. The returned
    /// request is the durable WAL form.
    pub(in crate::node) fn materialize_phrases(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        let Some(index) = &self.phrase_index else {
            if !doc.phrases.is_empty()
                || doc.phrase_fingerprint != 0
                || !doc.phrase_field.is_empty()
            {
                return Err(Status::failed_precondition(
                    "document carries derived phrase data but this node has no phrase glossary configured",
                ));
            }
            return Ok((doc, analyzed));
        };
        let expected = index.fingerprint();
        let fresh = doc.phrase_fingerprint == 0;
        if fresh {
            if !doc.phrases.is_empty() {
                return Err(Status::invalid_argument(
                    "client-supplied phrase postings require their non-zero vocabulary fingerprint",
                ));
            }
            doc.phrases = index.postings(&doc.text);
            doc.phrase_fingerprint = expected;
            doc.phrase_field = index.phrase_field().to_string();
        } else if doc.phrase_fingerprint != expected {
            return Err(Status::failed_precondition(format!(
                "document phrase fingerprint {:016x} differs from configured vocabulary {:016x}; rebuild or replay with the matching glossary",
                doc.phrase_fingerprint, expected
            )));
        } else if doc.phrase_field != index.phrase_field() {
            return Err(Status::failed_precondition(format!(
                "document phrase field {:?} differs from configured field {:?}",
                doc.phrase_field,
                index.phrase_field()
            )));
        }
        for posting in &doc.phrases {
            if posting.term != protomolt_analyzer::phrase_posting_term(&posting.concept_id)
                || posting.field != index.phrase_field()
                || posting.concept_id.is_empty()
                || posting.token_count == 0
                || posting.offsets.is_empty()
                || posting.offsets.iter().any(|span| span.start >= span.end)
            {
                return Err(Status::invalid_argument(format!(
                    "invalid durable phrase posting for concept {:?}",
                    posting.concept_id
                )));
            }
        }
        // A non-zero fingerprint marks a replay-ready document. Its entity
        // entries were already derived before the WAL append, so never run a
        // possibly changed NER model over it again.
        if fresh {
            let mut derived = index.glossary_entities(&doc.phrases);
            if let Some(field) = index.entity_map_field() {
                derived.extend(crate::phrases::ner_entities(field, &analyzed.entities));
            }
            for entry in derived {
                match doc
                    .map_facets
                    .iter()
                    .find(|held| held.field == entry.field && held.key == entry.key)
                {
                    Some(held) if held.value == entry.value => {}
                    Some(_) => {
                        return Err(Status::invalid_argument(format!(
                            "derived entity map key {:?} conflicts with a client value",
                            entry.key
                        )))
                    }
                    None => doc.map_facets.push(entry),
                }
            }
        }
        let field_index = self
            .config
            .bm25_fields
            .iter()
            .position(|field| field == index.phrase_field())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "configured phrase field {:?} is absent from this node's BM25 field table",
                    index.phrase_field()
                ))
            })?;
        if analyzed.fields.len() <= field_index {
            analyzed
                .fields
                .resize_with(field_index + 1, Default::default);
        }
        if analyzed.fields[field_index] != crate::postings::AnalyzedField::default() {
            return Err(Status::invalid_argument(format!(
                "derived phrase field {:?} collides with supplied analyzed data",
                index.phrase_field()
            )));
        }
        analyzed.fields[field_index] = crate::phrases::analyzed_field(&doc.phrases);
        Ok((doc, analyzed))
    }

    /// Admit a write only for this node's collection: an empty name means
    /// this node's collection, and any other name refuses.
    pub(in crate::node) fn admit_collection(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.config.collection {
            return Ok(());
        }
        Err(if self.config.collection.is_empty() {
            Status::invalid_argument(format!(
                "collection {requested:?} named, but this node serves no named collection"
            ))
        } else {
            Status::invalid_argument(format!(
                "this node serves collection {:?}, not {requested:?}",
                self.config.collection
            ))
        })
    }

    /// Derive token-position and bigram proximity payloads and pin the
    /// document's durable proximity record. Fresh records fill from node
    /// configuration; pre-populated records must match configuration and carry
    /// full positional term vectors.
    pub(in crate::node) fn materialize_proximity(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        use crate::proximity::{bigram_field_name, derive_bigrams};
        // Enforce single-collection membership and stamp unnamed documents before logging.
        self.admit_collection(&doc.collection)?;
        doc.collection = self.config.collection.clone();
        let configured_bigrams: Vec<crate::pb::BigramField> = self
            .config
            .bigram_fields
            .iter()
            .map(|source| crate::pb::BigramField {
                source: source.clone(),
                field: bigram_field_name(source),
            })
            .collect();
        let fresh = doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty();
        if fresh {
            doc.position_fields = self.config.position_fields.clone();
            doc.bigram_fields = configured_bigrams.clone();
            doc.sentence_fields = self.config.sentence_fields.clone();
        } else {
            let mut held: Vec<&str> = doc.sentence_fields.iter().map(String::as_str).collect();
            let mut want: Vec<&str> = self
                .config
                .sentence_fields
                .iter()
                .map(String::as_str)
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records sentence spans on {held:?} but this node keeps them on \
                     {want:?}; rebuild or replay with the matching --sentence-fields"
                )));
            }
            let mut held: Vec<&str> = doc.position_fields.iter().map(String::as_str).collect();
            let mut want: Vec<&str> = self
                .config
                .position_fields
                .iter()
                .map(String::as_str)
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records token positions on {held:?} but this node keeps them on \
                     {want:?}; rebuild or replay with the matching --position-fields"
                )));
            }
            let mut held: Vec<(&str, &str)> = doc
                .bigram_fields
                .iter()
                .map(|b| (b.source.as_str(), b.field.as_str()))
                .collect();
            let mut want: Vec<(&str, &str)> = configured_bigrams
                .iter()
                .map(|b| (b.source.as_str(), b.field.as_str()))
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records bigram columns {held:?} but this node derives {want:?}; \
                     rebuild or replay with the matching --bigram-fields"
                )));
            }
        }
        if doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty()
        {
            return Ok((doc, analyzed));
        }
        let table = &self.config.bm25_fields;
        let field_index = |name: &str| -> Result<usize, Status> {
            table.iter().position(|n| n == name).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "proximity field {name:?} is absent from this node's BM25 field table {table:?}"
                ))
            })
        };
        // The spec a field was analyzed under: the body's, or the extra
        // field's own. Positions need FULL vectors.
        let scoring_only = |fi: usize| -> bool {
            let spec = if fi == 0 {
                doc.analysis.as_ref()
            } else {
                doc.fields
                    .iter()
                    .find(|f| f.field == table[fi])
                    .and_then(|f| f.analysis.as_ref())
            };
            spec.is_some_and(|s| {
                s.term_vector_mode == crate::analyzer::TERM_VECTOR_MODE_SCORING_ONLY
            })
        };
        let require_positions = |analyzed: &crate::postings::AnalyzedDoc,
                                 fi: usize,
                                 role: &str|
         -> Result<(), Status> {
            let Some(field) = analyzed.fields.get(fi) else {
                // The document does not carry this field at all: there
                // is nothing to position and nothing to derive.
                return Ok(());
            };
            if field.terms.is_empty() {
                return Ok(());
            }
            if scoring_only(fi) {
                return Err(Status::invalid_argument(format!(
                    "{role} {:?} requires FULL term vectors: a SCORING_ONLY analysis has no \
                     occurrences to place, so no phrase could ever match it",
                    table[fi]
                )));
            }
            if field.positions.is_none() {
                return Err(Status::failed_precondition(format!(
                    "{role} {:?} needs token positions, but the analysis of this document carried \
                     no token layer to derive them from; positions are never guessed from spans",
                    table[fi]
                )));
            }
            field.check_positions().map_err(|error| {
                Status::invalid_argument(format!(
                    "{role} {:?}: malformed token positions: {error}",
                    table[fi]
                ))
            })
        };
        for name in &doc.position_fields {
            let fi = field_index(name)?;
            require_positions(&analyzed, fi, "positional field")?;
        }
        // Sentence fields require sorted, non-overlapping sentence spans covering
        // every indexed occurrence.
        for name in &doc.sentence_fields {
            let fi = field_index(name)?;
            let Some(field) = analyzed.fields.get(fi) else {
                continue;
            };
            if field.sentences.is_none() {
                return Err(Status::failed_precondition(format!(
                    "sentence field {name:?}: the analysis carried no sentence layer, so the \
                     document cannot be indexed with sentence spans; the analysis backend must \
                     return its sentence layer (the sidecar's sentence_detection)"
                )));
            }
            field.check_sentences().map_err(|error| {
                Status::failed_precondition(format!(
                    "sentence field {name:?}: malformed sentence spans: {error}"
                ))
            })?;
        }
        for bigram in &doc.bigram_fields {
            let source = field_index(&bigram.source)?;
            let derived = field_index(&bigram.field)?;
            if bigram.field != bigram_field_name(&bigram.source) {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} must be named {:?}",
                    bigram.field,
                    bigram_field_name(&bigram.source)
                )));
            }
            require_positions(&analyzed, source, "bigram source field")?;
            let Some(source_field) = analyzed.fields.get(source) else {
                continue;
            };
            if source_field.terms.is_empty() {
                continue;
            }
            let column = derive_bigrams(source_field).map_err(|error| {
                Status::invalid_argument(format!(
                    "bigram column {:?} from {:?}: {error}",
                    bigram.field, bigram.source
                ))
            })?;
            if analyzed.fields.len() <= derived {
                analyzed.fields.resize_with(derived + 1, Default::default);
            }
            if analyzed.fields[derived] != crate::postings::AnalyzedField::default() {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} collides with supplied analyzed data",
                    bigram.field
                )));
            }
            analyzed.fields[derived] = column;
        }
        Ok((doc, analyzed))
    }

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
