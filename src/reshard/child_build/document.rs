//! One document's analyzed fields, assembled for the child's field table.

use std::collections::BTreeMap;

use crate::pb::AddDocumentsRequest;
use crate::postings::{AnalyzedDoc, AnalyzedField, SpillBuilder};

/// A child's documents under their local slots, ascending.
pub(super) struct MappedDocuments {
    pub(super) rows: Vec<(u32, AddDocumentsRequest)>,
    /// Parent global id per child row: vector slots, then document-only
    /// rows in the order they were given.
    pub(super) row_parent_ids: Vec<u64>,
    pub(super) parent_of_local: BTreeMap<u32, u64>,
}

impl MappedDocuments {
    /// Give each document its vector's slot, or the next slot above the
    /// vector space when it has no vector.
    pub(super) fn assign(
        parent_ids: &[u64],
        documents: Vec<(u64, AddDocumentsRequest)>,
    ) -> Result<Self, String> {
        let slot_of: BTreeMap<u64, u32> = parent_ids
            .iter()
            .enumerate()
            .map(|(slot, &id)| (id, slot as u32))
            .collect();
        let mut rows = Vec::with_capacity(documents.len());
        let mut row_parent_ids = parent_ids.to_vec();
        let mut parent_of_local = BTreeMap::new();
        let mut spare = parent_ids.len() as u64;
        for (id, doc) in documents {
            let local = match slot_of.get(&id) {
                Some(&slot) => slot,
                None => {
                    spare += 1;
                    row_parent_ids.push(id);
                    u32::try_from(spare - 1)
                        .map_err(|_| "document id space exceeds u32".to_string())?
                }
            };
            parent_of_local.insert(local, id);
            rows.push((local, doc));
        }
        rows.sort_by_key(|(local, _)| *local);
        Ok(Self {
            rows,
            row_parent_ids,
            parent_of_local,
        })
    }
}

/// Where a document's analyzed fields come from: the analyzer's results
/// for this batch, in entry order, or a transplant from sealed segments
/// keyed by source id.
pub(super) enum FieldSource<'a> {
    Analyzed(std::vec::IntoIter<AnalyzedDoc>),
    Transplanted {
        fields: &'a mut BTreeMap<u64, Vec<AnalyzedField>>,
        parent_of_local: &'a BTreeMap<u32, u64>,
    },
}

fn position(table: &[String], name: &str) -> Option<usize> {
    table.iter().position(|n| n == name)
}

/// The fields of the document at `local`, one per table entry, with its
/// cased field, extra fields, and phrase field in place, and checked
/// against the positions and sentences the record says it keeps.
pub(super) fn assemble_fields(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    source: &mut FieldSource<'_>,
) -> Result<Vec<AnalyzedField>, String> {
    let mut fields = match source {
        FieldSource::Transplanted {
            fields,
            parent_of_local,
        } => transplanted_fields(table, local, doc, fields, parent_of_local)?,
        FieldSource::Analyzed(results) => analyzed_fields(table, local, doc, results)?,
    };
    place_phrases(table, local, doc, &mut fields)?;
    check_layers(table, local, doc, &fields)?;
    derive_bigrams(table, local, doc, &mut fields)?;
    Ok(fields)
}

/// Under a transplant the fields come from the spill's analysis sidecar
/// (`docs/replay-from-segments.md`); the record carries no extra field
/// texts, cased field, or phrases then, since those are fields of the
/// table like any other.
fn transplanted_fields(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    map: &mut BTreeMap<u64, Vec<AnalyzedField>>,
    parent_of_local: &BTreeMap<u32, u64>,
) -> Result<Vec<AnalyzedField>, String> {
    let id = parent_of_local[&local];
    let fields = map.remove(&id).ok_or_else(|| {
        format!(
            "record at child slot {local} (source id {id}) has no transplanted analysis in the \
             spill sidecar"
        )
    })?;
    if fields.len() != table.len() {
        return Err(format!(
            "record at child slot {local} (source id {id}): {} transplanted fields for a table \
             of {}",
            fields.len(),
            table.len()
        ));
    }
    if !doc.fields.is_empty() || !doc.cased_field.is_empty() || !doc.phrases.is_empty() {
        return Err(format!(
            "record at child slot {local} (source id {id}) carries field texts, a cased field, \
             or phrases under a transplant"
        ));
    }
    Ok(fields)
}

/// The body, its cased identity, and each extra field from the
/// analyzer's results, which hold one entry per text in record order.
fn analyzed_fields(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    results: &mut std::vec::IntoIter<AnalyzedDoc>,
) -> Result<Vec<AnalyzedField>, String> {
    let mut body = results.next().expect("counted by the batch");
    let cased = body.cased.take();
    let mut fields = vec![AnalyzedField::default(); table.len()];
    fields[0] = body.into_body();
    // The cased identity came out of the body's one pass; it lands at the
    // field the record named (docs/dual-cased.md).
    if !doc.cased_field.is_empty() {
        let Some(ci) = position(table, &doc.cased_field) else {
            return Err(format!(
                "record at child slot {local} names cased field {:?} outside the table \
                 {table:?}",
                doc.cased_field
            ));
        };
        fields[ci] = cased.ok_or_else(|| {
            format!(
                "record at child slot {local}: the replay analysis carried no cased identity \
                 for {:?}",
                doc.cased_field
            )
        })?;
    }
    for f in &doc.fields {
        let analyzed_field = results.next().expect("counted by the batch").into_body();
        let Some(fi) = position(table, &f.field) else {
            return Err(format!(
                "record at child slot {local} names field {:?} outside the table {table:?}",
                f.field
            ));
        };
        fields[fi] = analyzed_field;
    }
    Ok(fields)
}

fn place_phrases(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    fields: &mut [AnalyzedField],
) -> Result<(), String> {
    let Some(first) = doc.phrases.first() else {
        return Ok(());
    };
    let phrase_field = &first.field;
    if doc
        .phrases
        .iter()
        .any(|posting| &posting.field != phrase_field)
    {
        return Err(format!("record at child slot {local} mixes phrase fields"));
    }
    let Some(fi) = position(table, phrase_field) else {
        return Err(format!(
            "record at child slot {local} names phrase field {phrase_field:?} outside table \
             {table:?}"
        ));
    };
    fields[fi] = crate::phrases::analyzed_field(&doc.phrases);
    Ok(())
}

/// Proximity payloads (docs/phrase-proximity.md) and sentence spans
/// (docs/highlighting.md): a field the record keeps them on must have
/// them from the replay analysis; it is refused, never re-indexed
/// without them.
fn check_layers(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    fields: &[AnalyzedField],
) -> Result<(), String> {
    for name in &doc.position_fields {
        let fi = position(table, name).expect("checked against the table");
        if !fields[fi].terms.is_empty() && fields[fi].positions.is_none() {
            return Err(format!(
                "record at child slot {local}: field {name:?} keeps token positions but the \
                 replay analysis carried none"
            ));
        }
    }
    for name in &doc.sentence_fields {
        let fi = position(table, name).expect("checked against the table");
        if fields[fi].sentences.is_none() {
            return Err(format!(
                "record at child slot {local}: field {name:?} keeps sentence spans but the \
                 replay analysis carried none"
            ));
        }
        if let Err(error) = fields[fi].check_sentences() {
            return Err(format!(
                "record at child slot {local}: field {name:?}: malformed sentence spans: {error}"
            ));
        }
    }
    Ok(())
}

/// A bigram column derives from its source exactly as at first ingest.
fn derive_bigrams(
    table: &[String],
    local: u32,
    doc: &AddDocumentsRequest,
    fields: &mut [AnalyzedField],
) -> Result<(), String> {
    for bigram in &doc.bigram_fields {
        let Some(source) = position(table, &bigram.source) else {
            return Err(format!(
                "record at child slot {local} derives {:?} from {:?}, which is outside the table \
                 {table:?}",
                bigram.field, bigram.source
            ));
        };
        let derived =
            position(table, &bigram.field).expect("bigram columns joined the table above");
        if fields[source].terms.is_empty() {
            continue;
        }
        fields[derived] = crate::proximity::derive_bigrams(&fields[source]).map_err(|error| {
            format!(
                "record at child slot {local}: bigram column {:?} from {:?}: {error}",
                bigram.field, bigram.source
            )
        })?;
    }
    Ok(())
}

/// Record the analyzer fingerprint of every field the document fills:
/// the body's spec, the cased twin of it, each extra field's spec, the
/// phrase fingerprint, and each bigram column's derivation of its source.
pub(super) fn set_fingerprints(
    builder: &mut SpillBuilder,
    table: &[String],
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    use crate::analyzer::analysis_fingerprint;
    builder
        .set_analysis_fingerprint(0, analysis_fingerprint(doc.analysis.as_ref()))
        .map_err(|error| format!("body analysis fingerprint: {error}"))?;
    if let (false, Some(spec)) = (doc.cased_field.is_empty(), doc.analysis.as_ref()) {
        let ci = position(table, &doc.cased_field).expect("cased field was resolved above");
        let twin = crate::analyzer::cased_twin_spec(spec);
        builder
            .set_analysis_fingerprint(ci, analysis_fingerprint(Some(&twin)))
            .map_err(|error| {
                format!(
                    "cased field {:?} analysis fingerprint: {error}",
                    doc.cased_field
                )
            })?;
    }
    for field in &doc.fields {
        let fi = position(table, &field.field).expect("field was resolved above");
        builder
            .set_analysis_fingerprint(fi, analysis_fingerprint(field.analysis.as_ref()))
            .map_err(|error| format!("field {:?} analysis fingerprint: {error}", field.field))?;
    }
    if doc.phrase_fingerprint != 0 && !doc.phrase_field.is_empty() {
        let phrase_field = doc.phrase_field.as_str();
        let fi = position(table, phrase_field).expect("phrase field was resolved above");
        builder
            .set_analysis_fingerprint(fi, doc.phrase_fingerprint)
            .map_err(|error| format!("phrase field {phrase_field:?} fingerprint: {error}"))?;
    }
    for bigram in &doc.bigram_fields {
        let source = position(table, &bigram.source).expect("bigram source was resolved above");
        let derived = position(table, &bigram.field).expect("bigram column was resolved above");
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
    Ok(())
}

/// Add the document's text, original source, and fields at `local`.
pub(super) fn add_document(
    builder: &mut SpillBuilder,
    local: u32,
    doc: &mut AddDocumentsRequest,
    fields: Vec<AnalyzedField>,
) -> Result<(), String> {
    let text = std::mem::take(&mut doc.text);
    if let Some(source) = &doc.original_source {
        builder
            .source_archive_mut()
            .attach_source_with_identity(
                local,
                source,
                doc.source_chunk_ordinal,
                doc.identity.as_ref(),
            )
            .map_err(|e| format!("restore original source: {e}"))?;
    } else if doc.source_chunk_ordinal.is_some() || doc.identity.is_some() {
        return Err("source chunk ordinal or identity has no original source".into());
    }
    // Reshard replays LOGGED requests, whose quality and geography values
    // were materialized into the ordinary column lists at first ingest, so
    // there is nothing to derive here (docs/quality-columns.md,
    // docs/geography-columns.md).
    let analyzed = AnalyzedDoc {
        fields,
        ..Default::default()
    };
    let lineage = doc.lineage.map(|l| crate::postings::DocLineage {
        parent_id: l.parent_id,
        group_id: l.group_id,
        span_start: l.span_start,
        span_end: l.span_end,
    });
    builder
        .add_document_with_lineage(local, text, analyzed, lineage)
        .map_err(|e| format!("spill write (child slot {local}): {e}"))
}
