use super::compaction_build::{child_order, partition_key_of};
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn build_child(
    manifest: &WalManifest,
    vectors: Vec<(u64, Vec<f32>)>,
    documents: Vec<(u64, AddDocumentsRequest)>,
    vector_path: &Path,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    derived: Option<&crate::postings::StoredDerived>,
    columns: Option<&ColumnTables>,
    mut transplanted: Option<BTreeMap<u64, Vec<AnalyzedField>>>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    let dim = manifest.dim as usize;
    let backend_config = manifest
        .backend_config()
        .map_err(|e| format!("construct child backend config: {e}"))?;
    let mut index = VectorIndex::from_backend_config(dim, &backend_config)
        .map_err(|e| format!("construct child index: {e}"))?;
    let mut parent_ids = Vec::with_capacity(vectors.len());
    let mut flat = Vec::with_capacity(vectors.len() * dim);
    for (id, vector) in &vectors {
        parent_ids.push(*id);
        flat.extend_from_slice(vector);
    }
    index
        .add(&flat, dim)
        .map_err(|e| format!("add child vectors: {e}"))?;
    index
        .prepare()
        .map_err(|e| format!("prepare child index: {e}"))?;
    index
        .write(vector_path)
        .map_err(|e| format!("write {}: {e}", vector_path.display()))?;
    let exact_vector_path = crate::node::exact_vector_sidecar_path(vector_path);
    crate::exact_vectors::ExactVectorStore::from_values(dim, flat)
        .and_then(|store| store.write(&exact_vector_path))
        .map_err(|e| format!("write {}: {e}", exact_vector_path.display()))?;

    let slot_of: BTreeMap<u64, u32> = parent_ids
        .iter()
        .enumerate()
        .map(|(slot, &id)| (id, slot as u32))
        .collect();
    let mut mapped: Vec<(u32, AddDocumentsRequest)> = Vec::with_capacity(documents.len());
    let mut row_parent_ids = parent_ids.clone();
    let mut parent_of_local: BTreeMap<u32, u64> = BTreeMap::new();
    let mut spare = vectors.len() as u64;
    for (id, doc) in documents {
        let local = match slot_of.get(&id) {
            Some(&slot) => slot,
            None => {
                spare += 1;
                row_parent_ids.push(id);
                u32::try_from(spare - 1).map_err(|_| "document id space exceeds u32".to_string())?
            }
        };
        parent_of_local.insert(local, id);
        mapped.push((local, doc));
    }
    mapped.sort_by_key(|(local, _)| *local);
    let num_documents = mapped.len() as u64;

    let mut bm25_path = None;
    if !mapped.is_empty() {
        let analysis_contract = match binding {
            Some(binding) => crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )?,
            None => crate::pb::MappedAnalysisContract::default(),
        };
        let table: Vec<String> = match bm25_fields {
            Some(t) => t.to_vec(),
            None => {
                let mut extras = BTreeSet::new();
                for (_, doc) in &mapped {
                    for f in &doc.fields {
                        extras.insert(f.field.clone());
                    }
                    for phrase in &doc.phrases {
                        extras.insert(phrase.field.clone());
                    }
                    if !doc.phrase_field.is_empty() {
                        extras.insert(doc.phrase_field.clone());
                    }
                    for bigram in &doc.bigram_fields {
                        extras.insert(bigram.field.clone());
                    }
                }
                extras.extend(
                    analysis_contract
                        .fields
                        .iter()
                        .map(|field| field.name.clone()),
                );
                extras.remove("body");
                let mut t = vec!["body".to_string()];
                t.extend(extras);
                t
            }
        };
        if table.first().map(String::as_str) != Some("body") {
            return Err("bm25 field table must start with \"body\"".to_string());
        }
        let facet_table: Vec<String> = match columns {
            Some(columns) => columns.facets.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for fv in &doc.facets {
                        if !t.iter().any(|n| n == &fv.field) {
                            t.push(fv.field.clone());
                        }
                    }
                }
                t
            }
        };
        let numeric_table: Vec<String> = match columns {
            Some(columns) => columns.numerics.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for nv in &doc.numerics {
                        if !t.iter().any(|n| n == &nv.field) {
                            t.push(nv.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_facet_table: Vec<String> = match columns {
            Some(columns) => columns.map_facets.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_facets {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_numeric_table: Vec<String> = match columns {
            Some(columns) => columns.map_numerics.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_numerics {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_integer_table: Vec<String> = match columns {
            Some(columns) => columns.map_integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_integers {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_unsigned_integer_table: Vec<String> = match columns {
            Some(columns) => columns.map_unsigned_integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_unsigned_integers {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let integer_table: Vec<String> = match columns {
            Some(columns) => columns.integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for name in doc
                        .integers
                        .iter()
                        .map(|e| &e.field)
                        .chain(doc.timestamps.iter().map(|e| &e.field))
                    {
                        if !t.iter().any(|n| n == name) {
                            t.push(name.clone());
                        }
                    }
                }
                t
            }
        };
        let unsigned_integer_table: Vec<String> = match columns {
            Some(columns) => columns.unsigned_integers.clone(),
            None => {
                let mut names = Vec::new();
                for (_, doc) in &mapped {
                    for value in &doc.unsigned_integers {
                        if !names.contains(&value.field) {
                            names.push(value.field.clone());
                        }
                    }
                }
                names
            }
        };
        let geo_table: Vec<String> = match columns {
            Some(columns) => columns.geo.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.geo_points {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let path = crate::node::bm25_sidecar_path(vector_path);
        let mut spill_dir = path.as_os_str().to_owned();
        spill_dir.push(".build");
        let spill_dir = std::path::PathBuf::from(spill_dir);
        let names: Vec<&str> = table.iter().map(String::as_str).collect();
        let facet_names: Vec<&str> = facet_table.iter().map(String::as_str).collect();
        let numeric_names: Vec<&str> = numeric_table.iter().map(String::as_str).collect();
        let map_facet_names: Vec<&str> = map_facet_table.iter().map(String::as_str).collect();
        let map_numeric_names: Vec<&str> = map_numeric_table.iter().map(String::as_str).collect();
        let map_integer_names: Vec<&str> = map_integer_table.iter().map(String::as_str).collect();
        let map_unsigned_integer_names: Vec<&str> = map_unsigned_integer_table
            .iter()
            .map(String::as_str)
            .collect();
        let integer_names: Vec<&str> = integer_table.iter().map(String::as_str).collect();
        let unsigned_integer_names: Vec<&str> =
            unsigned_integer_table.iter().map(String::as_str).collect();
        let geo_names: Vec<&str> = geo_table.iter().map(String::as_str).collect();
        let position_table: Vec<String> = {
            let mut t = BTreeSet::new();
            for (local, doc) in &mapped {
                for name in &doc.position_fields {
                    if !table.contains(name) {
                        return Err(format!(
                            "record at child slot {local} keeps positions on {name:?} outside the \
                             table {table:?}"
                        ));
                    }
                    t.insert(name.clone());
                }
            }
            t.into_iter().collect()
        };
        let position_names: Vec<&str> = position_table.iter().map(String::as_str).collect();
        // The child's sentence fields (docs/highlighting.md), by the same
        // rule.
        let sentence_table: Vec<String> = {
            let mut t = BTreeSet::new();
            for (local, doc) in &mapped {
                for name in &doc.sentence_fields {
                    if !table.contains(name) {
                        return Err(format!(
                            "record at child slot {local} keeps sentence spans on {name:?} \
                             outside the table {table:?}"
                        ));
                    }
                    t.insert(name.clone());
                }
            }
            t.into_iter().collect()
        };
        let sentence_names: Vec<&str> = sentence_table.iter().map(String::as_str).collect();
        let mut builder = SpillBuilder::create_with_fields(&spill_dir, &names)
            .map_err(|e| format!("spill dir {}: {e}", spill_dir.display()))?
            .with_facet_fields(&facet_names)
            .with_numeric_fields(&numeric_names)
            .with_map_facet_fields(&map_facet_names)
            .with_map_numeric_fields(&map_numeric_names)
            .with_map_integer_fields(&map_integer_names)
            .with_map_unsigned_integer_fields(&map_unsigned_integer_names)
            .with_integer_fields(&integer_names)
            .with_unsigned_integer_fields(&unsigned_integer_names)
            .with_geo_fields(&geo_names)
            .with_position_fields(&position_names)
            .with_sentence_fields(&sentence_names);
        // A compaction pins the source shard's per-field analyzer
        // fingerprints on every output up front, so a bucket whose rows
        // never carry an optional field still records the field's
        // identity and the outputs open as one set; a record that
        // contradicts a pinned fingerprint refuses below as it would at
        // ingest.
        if let Some(pinned) = pinned_fingerprints {
            if pinned.len() != table.len() {
                return Err(format!(
                    "pinned analysis fingerprints cover {} fields but the table has {}",
                    pinned.len(),
                    table.len()
                ));
            }
            for (fi, fingerprint) in pinned.iter().enumerate() {
                builder
                    .set_analysis_fingerprint(fi, *fingerprint)
                    .map_err(|error| format!("pin field {:?}: {error}", table[fi]))?;
            }
        }
        for field in &analysis_contract.fields {
            let fi = table
                .iter()
                .position(|name| name == &field.name)
                .ok_or_else(|| {
                    format!(
                        "bound analysis field {:?} is absent from child table",
                        field.name
                    )
                })?;
            builder.set_analysis_fingerprint(
                fi,
                crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
            )?;
        }
        let mut i = 0;
        while i < mapped.len() {
            // Batch by document, one analyzer entry per field (body
            // first, extras in record order), batches aligned to
            // document boundaries so reassembly is positional.
            let mut end = i;
            let mut entries = 0usize;
            while end < mapped.len() && (entries == 0 || entries < ANALYZE_BATCH) {
                entries += 1 + mapped[end].1.fields.len();
                end += 1;
            }
            let analyzed = if transplanted.is_some() {
                Vec::new()
            } else {
                let mut batch: Vec<(&str, Option<&AnalysisSpec>, SessionLayers)> =
                    Vec::with_capacity(entries);
                for (_, d) in &mapped[i..end] {
                    // The layers each text needs on replay: sentence
                    // spans for a sentence field, and the cased identity
                    // for the body when the record names a cased field.
                    batch.push((
                        d.text.as_str(),
                        d.analysis.as_ref(),
                        SessionLayers {
                            sentences: !d.sentence_fields.is_empty(),
                            dual_cased: !d.cased_field.is_empty(),
                            ..SessionLayers::default()
                        },
                    ));
                    for f in &d.fields {
                        batch.push((
                            f.text.as_str(),
                            f.analysis.as_ref(),
                            SessionLayers {
                                sentences: d.sentence_fields.iter().any(|n| n == &f.field),
                                ..SessionLayers::default()
                            },
                        ));
                    }
                }
                analyze(&batch)
                    .map_err(|e| format!("analyze batch at child slot {}: {e}", mapped[i].0))?
            };
            if transplanted.is_none() && analyzed.len() != entries {
                return Err(format!(
                    "analyzer returned {} results for {entries} field texts",
                    analyzed.len()
                ));
            }
            let mut results = analyzed.into_iter();
            for (local, doc) in &mut mapped[i..end] {
                // Under a transplant the fields come from the spill's
                // analysis sidecar, keyed by the source id
                // (docs/replay-from-segments.md); the record carries no
                // extra field texts, cased field, or phrases then, since
                // those are fields of the table like any other.
                let (mut fields, cased) = match transplanted.as_mut() {
                    Some(map) => {
                        let id = parent_of_local[local];
                        let fields = map.remove(&id).ok_or_else(|| {
                            format!(
                                "record at child slot {local} (source id {id}) has no \
                                 transplanted analysis in the spill sidecar"
                            )
                        })?;
                        if fields.len() != table.len() {
                            return Err(format!(
                                "record at child slot {local} (source id {id}): {} transplanted \
                                 fields for a table of {}",
                                fields.len(),
                                table.len()
                            ));
                        }
                        if !doc.fields.is_empty()
                            || !doc.cased_field.is_empty()
                            || !doc.phrases.is_empty()
                        {
                            return Err(format!(
                                "record at child slot {local} (source id {id}) carries field \
                                 texts, a cased field, or phrases under a transplant"
                            ));
                        }
                        (fields, None)
                    }
                    None => {
                        let mut body = results.next().expect("counted above");
                        let cased = body.cased.take();
                        let mut fields =
                            vec![crate::postings::AnalyzedField::default(); table.len()];
                        fields[0] = body.into_body();
                        (fields, cased)
                    }
                };
                // The cased identity came out of the body's one pass; it
                // lands at the field the record named (docs/dual-cased.md).
                if !doc.cased_field.is_empty() {
                    let Some(ci) = table.iter().position(|n| n == &doc.cased_field) else {
                        return Err(format!(
                            "record at child slot {local} names cased field {:?} outside the \
                             table {table:?}",
                            doc.cased_field
                        ));
                    };
                    fields[ci] = cased.ok_or_else(|| {
                        format!(
                            "record at child slot {local}: the replay analysis carried no cased \
                             identity for {:?}",
                            doc.cased_field
                        )
                    })?;
                }
                for f in &doc.fields {
                    let analyzed_field = results.next().expect("counted above").into_body();
                    let Some(fi) = table.iter().position(|n| n == &f.field) else {
                        return Err(format!(
                            "record at child slot {local} names field {:?} outside the \
                             table {table:?}",
                            f.field
                        ));
                    };
                    fields[fi] = analyzed_field;
                }
                if !doc.phrases.is_empty() {
                    let phrase_field = &doc.phrases[0].field;
                    if doc
                        .phrases
                        .iter()
                        .any(|posting| &posting.field != phrase_field)
                    {
                        return Err(format!("record at child slot {local} mixes phrase fields"));
                    }
                    let Some(fi) = table.iter().position(|name| name == phrase_field) else {
                        return Err(format!(
                            "record at child slot {local} names phrase field {phrase_field:?} outside table {table:?}"
                        ));
                    };
                    fields[fi] = crate::phrases::analyzed_field(&doc.phrases);
                }
                // Proximity payloads (docs/phrase-proximity.md): the
                // re-analysis carries token positions from the same
                // fingerprinted tokenizer, so a positional field takes
                // them and a bigram column derives from its source
                // exactly as at first ingest. A field that needs
                // positions and got none is refused, never re-indexed
                // without them.
                for name in &doc.position_fields {
                    let fi = table.iter().position(|n| n == name).expect("checked above");
                    if !fields[fi].terms.is_empty() && fields[fi].positions.is_none() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?} keeps token positions \
                             but the replay analysis carried none"
                        ));
                    }
                }
                for name in &doc.sentence_fields {
                    let fi = table.iter().position(|n| n == name).expect("checked above");
                    if fields[fi].sentences.is_none() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?} keeps sentence spans \
                             but the replay analysis carried none"
                        ));
                    }
                    if let Err(error) = fields[fi].check_sentences() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?}: malformed sentence \
                             spans: {error}"
                        ));
                    }
                }
                for bigram in &doc.bigram_fields {
                    let Some(source) = table.iter().position(|n| n == &bigram.source) else {
                        return Err(format!(
                            "record at child slot {local} derives {:?} from {:?}, which is outside \
                             the table {table:?}",
                            bigram.field, bigram.source
                        ));
                    };
                    let derived = table
                        .iter()
                        .position(|n| n == &bigram.field)
                        .expect("bigram columns joined the table above");
                    if fields[source].terms.is_empty() {
                        continue;
                    }
                    fields[derived] = crate::proximity::derive_bigrams(&fields[source]).map_err(
                        |error| {
                            format!(
                                "record at child slot {local}: bigram column {:?} from {:?}: {error}",
                                bigram.field, bigram.source
                            )
                        },
                    )?;
                }
                builder
                    .set_analysis_fingerprint(
                        0,
                        crate::analyzer::analysis_fingerprint(doc.analysis.as_ref()),
                    )
                    .map_err(|error| format!("body analysis fingerprint: {error}"))?;
                // The cased field carries the twin of the body's spec, as
                // at first ingest (docs/dual-cased.md).
                if let (false, Some(spec)) = (doc.cased_field.is_empty(), doc.analysis.as_ref()) {
                    let ci = table
                        .iter()
                        .position(|name| name == &doc.cased_field)
                        .expect("cased field was resolved above");
                    let twin = crate::analyzer::cased_twin_spec(spec);
                    builder
                        .set_analysis_fingerprint(
                            ci,
                            crate::analyzer::analysis_fingerprint(Some(&twin)),
                        )
                        .map_err(|error| {
                            format!(
                                "cased field {:?} analysis fingerprint: {error}",
                                doc.cased_field
                            )
                        })?;
                }
                for field in &doc.fields {
                    let fi = table
                        .iter()
                        .position(|name| name == &field.field)
                        .expect("field was resolved above");
                    builder
                        .set_analysis_fingerprint(
                            fi,
                            crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
                        )
                        .map_err(|error| {
                            format!("field {:?} analysis fingerprint: {error}", field.field)
                        })?;
                }
                if doc.phrase_fingerprint != 0 && !doc.phrase_field.is_empty() {
                    let phrase_field = doc.phrase_field.as_str();
                    let fi = table
                        .iter()
                        .position(|name| name == phrase_field)
                        .expect("phrase field was resolved above");
                    builder
                        .set_analysis_fingerprint(fi, doc.phrase_fingerprint)
                        .map_err(|error| {
                            format!("phrase field {phrase_field:?} fingerprint: {error}")
                        })?;
                }
                for bigram in &doc.bigram_fields {
                    let source = table
                        .iter()
                        .position(|n| n == &bigram.source)
                        .expect("bigram source was resolved above");
                    let derived = table
                        .iter()
                        .position(|n| n == &bigram.field)
                        .expect("bigram column was resolved above");
                    let source_fingerprint = builder.analysis_fingerprint(source);
                    if source_fingerprint != 0 {
                        builder
                            .set_analysis_fingerprint(
                                derived,
                                crate::proximity::bigram_fingerprint(source_fingerprint),
                            )
                            .map_err(|error| {
                                format!("bigram column {:?} fingerprint: {error}", bigram.field)
                            })?;
                    }
                }
                let text = std::mem::take(&mut doc.text);
                if let Some(source) = &doc.original_source {
                    builder
                        .source_archive_mut()
                        .attach_source_with_identity(
                            *local,
                            source,
                            doc.source_chunk_ordinal,
                            doc.identity.as_ref(),
                        )
                        .map_err(|e| format!("restore original source: {e}"))?;
                } else if doc.source_chunk_ordinal.is_some() || doc.identity.is_some() {
                    return Err("source chunk ordinal or identity has no original source".into());
                }
                builder
                    .add_document_with_lineage(
                        *local,
                        text,
                        // Reshard replays LOGGED requests, whose quality
                        // and geography values were materialized into the
                        // ordinary column lists at first ingest, so there
                        // is nothing to derive here
                        // (docs/quality-columns.md,
                        // docs/geography-columns.md).
                        crate::postings::AnalyzedDoc {
                            cased: None,
                            fields,
                            quality: None,
                            geography: None,
                            entities: Vec::new(),
                        },
                        doc.lineage.map(|l| crate::postings::DocLineage {
                            parent_id: l.parent_id,
                            group_id: l.group_id,
                            span_start: l.span_start,
                            span_end: l.span_end,
                        }),
                    )
                    .map_err(|e| format!("spill write (child slot {local}): {e}"))?;
                for fv in &doc.facets {
                    let fi = facet_table
                        .iter()
                        .position(|n| n == &fv.field)
                        .expect("facet table was derived from these records");
                    builder.set_facet(fi, *local, &fv.value);
                }
                for nv in &doc.numerics {
                    let ni = numeric_table
                        .iter()
                        .position(|n| n == &nv.field)
                        .expect("numeric table was derived from these records");
                    builder.set_numeric(ni, *local, nv.value);
                }
                for e in &doc.map_facets {
                    let ci = map_facet_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("map-facet table was derived from these records");
                    builder.set_map_facet(ci, *local, &e.key, &e.value);
                }
                for e in &doc.map_numerics {
                    let ci = map_numeric_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("map-numeric table was derived from these records");
                    builder.set_map_numeric(ci, *local, &e.key, e.value);
                }
                for e in &doc.map_integers {
                    let ci = map_integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .ok_or_else(|| {
                            format!(
                                "integer map column {:?} is absent from the replay table",
                                e.field
                            )
                        })?;
                    builder
                        .set_map_integer(ci, *local, &e.key, e.value)
                        .map_err(|e| format!("integer map replay: {e}"))?;
                }
                for e in &doc.map_unsigned_integers {
                    let ci = map_unsigned_integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .ok_or_else(|| {
                            format!(
                                "integer map column {:?} is absent from the replay table",
                                e.field
                            )
                        })?;
                    builder
                        .set_map_unsigned_integer(ci, *local, &e.key, e.value)
                        .map_err(|e| format!("integer map replay: {e}"))?;
                }
                for e in &doc.integers {
                    let ii = integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("integer table was derived from these records");
                    builder.set_integer(ii, *local, e.value);
                }
                let mut unsigned_seen = std::collections::HashSet::new();
                for value in &doc.unsigned_integers {
                    if !unsigned_seen.insert(&value.field) {
                        return Err(format!(
                            "record at child slot {local}: unsigned field {:?} repeats",
                            value.field
                        ));
                    }
                    let column = unsigned_integer_table
                        .iter()
                        .position(|name| name == &value.field)
                        .ok_or_else(|| {
                            format!(
                                "record at child slot {local}: undeclared unsigned field {:?}",
                                value.field
                            )
                        })?;
                    builder.set_unsigned_integer(column, *local, value.value);
                }
                // Timestamps re-run the node's conversion, from the
                // instant the WAL kept: the child gets the same epoch
                // micros the parent stored, not a copy of a copy.
                for e in &doc.timestamps {
                    let ii = integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("integer table was derived from these records");
                    let ts = e.value.as_ref().ok_or_else(|| {
                        format!(
                            "record at child slot {local}: timestamp field {:?} carries no \
                             instant",
                            e.field
                        )
                    })?;
                    let micros = crate::node::timestamp_to_epoch_micros(&e.field, ts)
                        .map_err(|e| format!("record at child slot {local}: {}", e.message()))?;
                    builder.set_integer(ii, *local, micros);
                }
                for e in &doc.geo_points {
                    let gi = geo_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("geo table was derived from these records");
                    builder.set_geo(gi, *local, e.lat, e.lon);
                }
            }
            i = end;
        }
        // The children stay bound to the plan the parents were written
        // under; replay must not launder a binding away.
        builder.set_binding(binding.cloned());
        // Likewise the derived-column declaration (docs/derived-columns.md):
        // the kind-15 entry names what the rows' derived values mean.
        builder.set_derived(derived.cloned());
        builder
            .finish(&path)
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        bm25_path = Some(path);
    }

    Ok(ChildImage {
        vector_path: vector_path.to_path_buf(),
        exact_vector_path,
        bm25_path,
        slot_offset: 0, // filled in by the caller
        hash_lo: 0,
        hash_hi: 0,
        num_vectors: vectors.len() as u64,
        num_documents,
        parent_ids,
        row_parent_ids,
    })
}

/// Assemble one child: replay its vectors/documents into an image under
/// `out_dir` named `shard-<ordinal>.vector`.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_child(
    manifest: &WalManifest,
    replay: Replay,
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    binding: Option<&crate::postings::StoredBinding>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    // A hash split keeps the parents' declaration, which the log's
    // manifest records (docs/derived-columns.md).
    let derived = manifest_derived(manifest)?;
    finish_child_pinned(
        manifest,
        replay,
        ordinal,
        out_dir,
        slot_offset,
        hash_lo,
        hash_hi,
        bm25_fields,
        None,
        binding,
        derived.as_ref(),
        None,
        analyze,
    )
}

/// The derived-column declaration a log's manifest records, in the
/// store's form; `None` for a log without one. A manifest whose
/// fingerprint does not cover its own `[[derived]]` table was edited by
/// hand and is refused.
pub fn manifest_derived(
    manifest: &WalManifest,
) -> Result<Option<crate::postings::StoredDerived>, String> {
    if manifest.derived.is_empty() {
        if !manifest.derived_fingerprint.is_empty() {
            return Err(format!(
                "the manifest records derived fingerprint {:?} and no [[derived]] table",
                manifest.derived_fingerprint
            ));
        }
        return Ok(None);
    }
    let spec = crate::derived::spec_from_config(&manifest.derived)?;
    let declaration = crate::derived::Declaration::compile(&spec)?;
    if declaration.fingerprint() != manifest.derived_fingerprint {
        return Err(format!(
            "the manifest's [[derived]] table hashes to {:?}, its recorded fingerprint is {:?}",
            declaration.fingerprint(),
            manifest.derived_fingerprint
        ));
    }
    Ok(Some(crate::postings::StoredDerived::of(&declaration)))
}

/// [`finish_child`] with the analyzer fingerprints a compaction pins.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_child_pinned(
    manifest: &WalManifest,
    mut replay: Replay,
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    derived: Option<&crate::postings::StoredDerived>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let order = child_order(&replay);
    finish_child_ordered(
        manifest,
        replay,
        &order,
        ordinal,
        out_dir,
        slot_offset,
        hash_lo,
        hash_hi,
        bm25_fields,
        pinned_fingerprints,
        binding,
        derived,
        columns,
        analyze,
    )
}

/// [`finish_child_pinned`] with the child's slot order given: `order`
/// lists the live source ids, vector-bearing rows first and document-only
/// rows after them (the shape [`build_child`] assigns slots in), and the
/// image takes its slots in that sequence. [`emit_rows_in`] with the same
/// order writes the matching log.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_child_ordered(
    manifest: &WalManifest,
    mut replay: Replay,
    order: &[u64],
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    derived: Option<&crate::postings::StoredDerived>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let vector_path = out_dir.join(format!("shard-{ordinal}.vector"));
    eprintln!(
        "reshard: child {ordinal}: {} vectors, {} documents -> {}",
        replay.vectors.len(),
        replay.documents.len(),
        vector_path.display()
    );
    let mut vectors = Vec::with_capacity(replay.vectors.len());
    let mut documents = Vec::with_capacity(replay.documents.len());
    for id in order {
        if let Some(vector) = replay.vectors.remove(id) {
            vectors.push((*id, vector));
        }
        if let Some(document) = replay.documents.remove(id) {
            documents.push((*id, document));
        }
    }
    if !replay.vectors.is_empty() || !replay.documents.is_empty() {
        return Err(format!(
            "child {ordinal}: the slot order names {} rows but the replay holds {} more",
            order.len(),
            replay.vectors.len() + replay.documents.len()
        ));
    }
    let transplanted = (!replay.analyzed.is_empty()).then(|| std::mem::take(&mut replay.analyzed));
    let mut child = build_child(
        manifest,
        vectors,
        documents,
        &vector_path,
        bm25_fields,
        pinned_fingerprints,
        binding,
        derived,
        columns,
        transplanted,
        analyze,
    )?;
    child.slot_offset = slot_offset;
    child.hash_lo = hash_lo;
    child.hash_hi = hash_hi;
    Ok(child)
}

/// One child of a placement-keyed split: the inclusive code range it
/// takes (`docs/placement.md`, "Changing the tree"). A single code is a
/// range of one; a prefix range takes a subtree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementChild {
    pub lo: i64,
    pub hi: i64,
}

/// The result of a placement-keyed split: the images plus, per child,
/// the code range it holds, and the cutoffs a live catch-up resumes from.
#[derive(Debug)]
pub struct PlacementReshardOutput {
    pub images: ReshardOutput,
    pub source_cutoffs: Vec<WalCutoff>,
    /// Parallel to `images.children`.
    pub ranges: Vec<PlacementChild>,
    /// The child that took the rows with no value in the column, when
    /// one was named.
    pub default_child: Option<usize>,
}

impl PlacementChild {
    /// A child with no code range of its own: only the default child may
    /// be one, and it takes the rows no range covers.
    pub const NONE: PlacementChild = PlacementChild { lo: 0, hi: -1 };

    pub fn is_none(&self) -> bool {
        self.lo > self.hi
    }
}

/// Validate a placement split's children: at least one, no range
/// inverted (a rangeless child is allowed only as the default), none
/// overlapping, and a default index inside the list.
pub fn validate_placement_children(
    children: &[PlacementChild],
    default_child: Option<usize>,
) -> Result<(), String> {
    if children.is_empty() {
        return Err("a placement split needs at least one child".to_string());
    }
    for (i, child) in children.iter().enumerate() {
        if child.is_none() {
            if default_child != Some(i) {
                return Err(format!(
                    "placement child {i} has no code range and is not the default child"
                ));
            }
            continue;
        }
        if child.lo < 0 {
            return Err(format!(
                "placement child {i} starts below zero ({}); codes are never negative",
                child.lo
            ));
        }
    }
    let mut sorted: Vec<(usize, PlacementChild)> = children
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, c)| !c.is_none())
        .collect();
    sorted.sort_by_key(|(_, c)| c.lo);
    for pair in sorted.windows(2) {
        let (i, a) = pair[0];
        let (j, b) = pair[1];
        if b.lo <= a.hi {
            return Err(format!(
                "placement children {i} ({}..={}) and {j} ({}..={}) overlap",
                a.lo, a.hi, b.lo, b.hi
            ));
        }
    }
    if let Some(d) = default_child {
        if d >= children.len() {
            return Err(format!(
                "default child {d} is outside the {} children",
                children.len()
            ));
        }
    }
    Ok(())
}

/// Split one or more full-history generations into children keyed by
/// the placement code each logged document carries in `column`
/// (`docs/placement.md`). No CEL runs at replay: the code was written at
/// ingest and the log holds it. A document whose code falls in no
/// child's range, a document with no value, and a vector row with no
/// document all go to `default_child`; without one, any such row
/// refuses the split by id. Every child keeps the parent's stable-key
/// hash coverage (the whole space), because routing under a tree is by
/// code first and by hash inside the group.
#[allow(clippy::too_many_arguments)]
pub fn split_placement_logs(
    gens: &[PathBuf],
    column: &str,
    children: &[PlacementChild],
    default_child: Option<usize>,
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<PlacementReshardOutput, String> {
    if column.trim().is_empty() {
        return Err("a placement split needs the placement column's name".to_string());
    }
    validate_placement_children(children, default_child)?;
    if slot_offsets.len() != children.len() {
        return Err(format!(
            "a placement split needs one slot offset per child: {} offsets for {} children",
            slot_offsets.len(),
            children.len()
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("a placement split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let child_of = |id: u64, code: Option<i64>| -> Result<usize, String> {
        if let Some(code) = code {
            if let Some(i) = children
                .iter()
                .position(|child| code >= child.lo && code <= child.hi)
            {
                return Ok(i);
            }
        }
        default_child.ok_or_else(|| match code {
            Some(code) => format!(
                "live source id {id} carries placement code {code}, outside every child's \
                 range, and the split names no default child"
            ),
            None => format!(
                "live source id {id} carries no value in {column:?} and the split names no \
                 default child"
            ),
        })
    };
    let mut codes: BTreeMap<u64, Option<i64>> = BTreeMap::new();
    for (id, document) in &replay.documents {
        codes.insert(*id, partition_key_of(document, column)?);
    }
    let mut buckets: Vec<Replay> = children.iter().map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].vectors.insert(id, vector);
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    for (id, document) in replay.documents {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].documents.insert(id, document);
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    let mut out = Vec::with_capacity(children.len());
    for (shard, replay) in buckets.into_iter().enumerate() {
        out.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_offsets[shard],
            0,
            u64::MAX,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(PlacementReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children: out,
        },
        source_cutoffs: cutoffs,
        ranges: children.to_vec(),
        default_child,
    })
}

/// The shard map for a placement split: `placement = <code>` on a child
/// that holds one code; a child holding a range is left for the
/// operator to place, with the range in a comment, because the map
/// names one code per shard.
pub fn placement_shard_map_toml(out: &PlacementReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.images.generation);
    for (child, range) in out.images.children.iter().zip(&out.ranges) {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
        if range.is_none() {
            s.push_str("# the default child: rows with no code or a code no child covers\n");
        } else if range.lo == range.hi {
            s.push_str(&format!("placement = {}\n", range.lo));
        } else {
            s.push_str(&format!(
                "# placement codes {}..={}: one code per shard in the map; choose one\n",
                range.lo, range.hi
            ));
        }
    }
    s
}
