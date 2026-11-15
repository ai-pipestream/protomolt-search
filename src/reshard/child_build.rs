//! Building one child image from replayed records, and the steps that seal it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::analyzer::SessionLayers;
use crate::pb::{AddDocumentsRequest, AnalysisSpec};
use crate::postings::{AnalyzedField, SpillBuilder};
use crate::vector::VectorIndex;
use crate::wal::WalManifest;

#[cfg(doc)]
use super::compact_log::emit_rows_in;
use super::hnsw_sources::HnswCompactionSources;
use super::replay::Replay;
use super::{Analyzer, ChildImage, ColumnTables, ANALYZE_BATCH};

/// Build one child image from its share of the replay: the vector index
/// (constructed from manifest provider state, vectors in id order), the BM25
/// sidecar from the documents, and the two output files.
///
/// Document local ids mirror the vector side's dense remap: a document
/// whose id also names a vector gets that vector's child slot (the
/// aligned-ingest case stays aligned); documents with no vector (the doc
/// side ran ahead) are appended above the vector space in id order.
/// `columns` gives the image its column tables; without them the
/// tables are derived from the records.
#[allow(clippy::too_many_arguments)]
fn build_child(
    manifest: &WalManifest,
    vectors: Vec<(u64, Vec<f32>)>,
    mut retained: BTreeMap<u64, Vec<u8>>,
    documents: Vec<(u64, AddDocumentsRequest)>,
    vector_path: &Path,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    derived: Option<&crate::postings::StoredDerived>,
    columns: Option<&ColumnTables>,
    mut transplanted: Option<BTreeMap<u64, Vec<AnalyzedField>>>,
    hnsw_sources: Option<&HnswCompactionSources>,
    prebuilt_index: Option<VectorIndex>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    let dim = manifest.dim as usize;
    let backend_config = manifest
        .backend_config()
        .map_err(|e| format!("construct child backend config: {e}"))?;
    let mut parent_ids = Vec::with_capacity(vectors.len());
    let mut flat = Vec::with_capacity(vectors.len() * dim);
    for (id, vector) in &vectors {
        parent_ids.push(*id);
        flat.extend_from_slice(vector);
    }
    let mut remapped = crate::multi_res::MultiResVectors::default();
    for (slot, (id, vector)) in vectors.iter().enumerate() {
        if let Some(bytes) = retained.remove(id) {
            let batch = crate::pb::AddVectorsRequest {
                vectors: vector.clone(),
                dim: manifest.dim,
                materialized_rows: vec![bytes],
            };
            let row = crate::multi_res_wal::decode_batch(&batch)?
                .into_iter()
                .next()
                .ok_or("materialized child row is missing")?;
            let slot = u32::try_from(slot).map_err(|_| "materialized child slot exceeds u32")?;
            remapped.insert(slot, row);
        }
    }
    if !retained.is_empty() {
        return Err("materialized rows have no matching child vectors".into());
    }
    let index = match prebuilt_index {
        Some(index) => index,
        None => {
            let merged_hnsw = match hnsw_sources {
                Some(sources) => sources.build_merged_index(dim, &backend_config, &vectors)?,
                None => None,
            };
            match merged_hnsw {
                Some(index) => index,
                None => {
                    let mut index = VectorIndex::from_backend_config(dim, &backend_config)
                        .map_err(|e| format!("construct child index: {e}"))?;
                    index
                        .add(&flat, dim)
                        .map_err(|e| format!("add child vectors: {e}"))?;
                    index
                        .prepare()
                        .map_err(|e| format!("prepare child index: {e}"))?;
                    index
                }
            }
        }
    };
    if !remapped.is_empty() {
        crate::live_docs::LiveDocs::default()
            .with_retained_vectors()
            .write(
                &crate::node::live_docs_sidecar_path(vector_path),
                vectors.len() as u64,
            )
            .map_err(|error| format!("publish retained child reader gate: {error}"))?;
    }
    index
        .write(vector_path)
        .map_err(|e| format!("write {}: {e}", vector_path.display()))?;
    let exact_vector_path = crate::node::exact_vector_sidecar_path(vector_path);
    crate::exact_vectors::ExactVectorStore::from_values(dim, flat)
        .and_then(|store| store.write(&exact_vector_path))
        .map_err(|e| format!("write {}: {e}", exact_vector_path.display()))?;

    // parent id -> child local slot, for the document remap.
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
        // The child field table (docs/multi-field.md): the caller's
        // fleet table when given, else "body" plus every extra field
        // name in this child's records in canonical lexical order. First-sight
        // order is not sufficient: a sparse optional field can first appear
        // after an always-present phrase field in one hash child and before it
        // in another, producing incompatible positional schemas. Old logs
        // carry no extra fields and derive the single-field table.
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
        // The child's facet and numeric tables are derived from the
        // replayed records (first-seen order): the WAL is the durable
        // column record, so a field no record values never reaches the
        // child.
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
        // Integers and timestamps name the SAME i64 columns
        // (docs/range-facets.md), so both lists feed one table — a
        // child whose records only ever carried timestamps still gets
        // the column, or the re-apply below would have nowhere to put
        // them.
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
        // Geo columns come from one list, so the derivation is the
        // plain first-seen union (docs/geo-columns.md).
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
        // Children rebuild through the disk spiller for the same reason
        // nodes do: a full-scale child's postings do not fit in heap.
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
        // The child's positional fields (docs/phrase-proximity.md) come
        // from the records' proximity record, which every record of one
        // generation carries identically; a field it names outside the
        // table is a corrupt record, refused.
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
                            fields,
                            ..Default::default()
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

    let payload = crate::multi_res::sidecar_path(vector_path);
    // An empty publication also retires stale retention when an output path
    // is reused. Segment metadata omits the artifact for an empty map.
    crate::multi_res_storage::write(&remapped, &payload, row_parent_ids.len() as u64, false)
        .map_err(|error| format!("write {}: {error}", payload.display()))?;
    if payload.try_exists().map_err(|error| error.to_string())? {
        crate::live_docs::LiveDocs::default()
            .with_retained_vectors()
            .write(
                &crate::node::live_docs_sidecar_path(vector_path),
                row_parent_ids.len() as u64,
            )
            .map_err(|error| format!("write retained generation reader gate: {error}"))?;
    }
    let multi_res_path = (!remapped.is_empty()).then_some(payload);
    Ok(ChildImage {
        multi_res_path,
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
    replay: Replay,
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
    finish_child_pinned_with_sources(
        manifest,
        replay,
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
        None,
        analyze,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_child_pinned_with_sources(
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
    hnsw_sources: Option<&HnswCompactionSources>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let order = child_order(&replay);
    finish_child_ordered_with_sources(
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
        hnsw_sources,
        analyze,
    )
}

/// [`finish_child_pinned`] with the child's slot order given: `order`
/// lists the live source ids, vector-bearing rows first and document-only
/// rows after them (the shape [`build_child`] assigns slots in), and the
/// image takes its slots in that sequence. [`emit_rows_in`] with the same
/// order writes the matching log.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_child_ordered_with_sources(
    manifest: &WalManifest,
    replay: Replay,
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
    hnsw_sources: Option<&HnswCompactionSources>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    finish_child_ordered_with_prebuilt(
        manifest,
        replay,
        order,
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
        hnsw_sources,
        None,
        analyze,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn finish_child_ordered_with_prebuilt(
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
    hnsw_sources: Option<&HnswCompactionSources>,
    prebuilt_index: Option<VectorIndex>,
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
    let transplanted = replay.analyzed.take_transplant(&documents)?;
    let mut child = build_child(
        manifest,
        vectors,
        replay.retained,
        documents,
        &vector_path,
        bm25_fields,
        pinned_fingerprints,
        binding,
        derived,
        columns,
        transplanted,
        hnsw_sources,
        prebuilt_index,
        analyze,
    )?;
    child.slot_offset = slot_offset;
    child.hash_lo = hash_lo;
    child.hash_hi = hash_hi;
    Ok(child)
}

/// The slot order [`build_child`] assigns a replay by default: vector
/// rows in source-id order, then the document-only rows in source-id
/// order.
pub(super) fn child_order(replay: &Replay) -> Vec<u64> {
    let mut order: Vec<u64> = replay.vectors.keys().copied().collect();
    order.extend(
        replay
            .documents
            .keys()
            .filter(|id| !replay.vectors.contains_key(id))
            .copied(),
    );
    order
}
