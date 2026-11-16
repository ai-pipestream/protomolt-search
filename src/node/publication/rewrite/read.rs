//! Budgeted reads of one row interval from a source segment.
use super::*;
use crate::postings::{FieldView, FileImpacts};
use crate::reshard::SourceTables;

/// The rows of one interval and the batch budget they are charged to.
struct Batch<'a> {
    reader: &'a Bm25Reader,
    start: u32,
    end: u32,
    rows: Vec<Option<Row>>,
    used: usize,
    limit: usize,
}

// Scans only posting skip runs intersecting this row interval. The budget is
// charged before retaining each row/term; no whole-field transpose is built.
pub(super) fn read_batch(
    before: &OpenedSegmentSet,
    segment: usize,
    start: u32,
    end: u32,
    limit: usize,
) -> Result<Vec<Option<Row>>, Status> {
    let reader = before.bm25(segment);
    let tables = segment_tables(reader);
    let mut used = 0;
    charge(
        &mut used,
        (end - start) as usize * std::mem::size_of::<Option<Row>>(),
        limit,
    )?;
    let mut batch = Batch {
        reader,
        start,
        end,
        rows: Vec::with_capacity((end - start) as usize),
        used,
        limit,
    };
    for row in start..end {
        if before.live_docs(segment).is_deleted(row as usize) {
            batch.rows.push(None);
            continue;
        }
        let row = batch.read_row(before, segment, row, &tables)?;
        batch.rows.push(Some(row));
    }
    for fi in 0..reader.field_count() {
        batch.shape_field(fi)?;
        batch.read_splade(fi)?;
        batch.read_postings(fi)?;
    }
    Ok(batch.rows)
}

impl Batch<'_> {
    /// One live row's stored document, vector and retained vectors, charged
    /// to the batch before it is kept.
    fn read_row(
        &mut self,
        before: &OpenedSegmentSet,
        segment: usize,
        row: u32,
        tables: &SourceTables,
    ) -> Result<Row, Status> {
        let (reader, limit) = (self.reader, self.limit);
        let document = reconstruct_document(reader, row, tables, true).map_err(failure)?;
        if document.original_source.is_none() || document.identity.is_none() {
            return Err(failure(
                "a live source row lacks its original source or identity",
            ));
        }
        let vector = before
            .exact_vectors(segment)
            .map(|exact| {
                exact
                    .row_values(row as usize, row as usize + 1)
                    .map_err(failure)
            })
            .transpose()?;
        let mut bytes = std::mem::size_of::<Row>();
        charge(&mut bytes, document.encoded_len(), limit)?;
        charge(
            &mut bytes,
            reader.field_count() * std::mem::size_of::<AnalyzedField>(),
            limit,
        )?;
        if let Some(vector) = &vector {
            charge(&mut bytes, vector.len() * std::mem::size_of::<f32>(), limit)?;
        }
        let materialized = before
            .retained_segment(segment)
            .retained(row)
            .map(|retained| {
                let size = crate::multi_res::encoded_len(retained).map_err(failure)?;
                charge(&mut bytes, size, limit)?;
                charge(&mut self.used, bytes, limit)?;
                crate::multi_res::encode(retained).map_err(failure)
            })
            .transpose()?;
        if materialized.is_none() {
            charge(&mut self.used, bytes, limit)?;
        }
        Ok(Row {
            materialized,
            document,
            fields: vec![AnalyzedField::default(); reader.field_count()],
            vector,
            bytes,
        })
    }

    /// Give every kept row its empty analysis of field `fi`.
    fn shape_field(&mut self, fi: usize) -> Result<(), Status> {
        let (reader, limit) = (self.reader, self.limit);
        let field = reader.field(fi);
        for (ordinal, row) in self.rows.iter_mut().enumerate() {
            let Some(row) = row else { continue };
            let original = self.start + ordinal as u32;
            let sentences = reader.field_doc_sentences(fi, original);
            let bytes = sentences.as_ref().map_or(0, |s| s.len() * 8);
            charge(&mut self.used, bytes, limit)?;
            charge(&mut row.bytes, bytes, limit)?;
            row.fields[fi] = AnalyzedField {
                terms: Vec::new(),
                length: field.doc_length(original),
                positions: reader.field_has_positions(fi).then(Vec::new),
                pos_lengths: field.has_pos_lengths().then(Vec::new),
                sentences,
                ..Default::default()
            };
        }
        Ok(())
    }

    fn read_splade(&mut self, fi: usize) -> Result<(), Status> {
        let (start, end, limit) = (self.start, self.end, self.limit);
        let (rows, used) = (&mut self.rows, &mut self.used);
        self.reader
            .field(fi)
            .visit_splade(start, end, |term, original, impact, scale| {
                if let Some(row) = &mut rows[(original - start) as usize] {
                    let bytes = std::mem::size_of::<(String, u16)>() + term.len();
                    charge(used, bytes, limit)?;
                    charge(&mut row.bytes, bytes, limit)?;
                    row.fields[fi].splade.push((term.to_owned(), impact));
                    row.fields[fi].splade_scale_bits = scale;
                }
                Ok::<(), Status>(())
            })?;
        Ok(())
    }

    fn read_postings(&mut self, fi: usize) -> Result<(), Status> {
        let field = self.reader.field(fi);
        for term in field.prefix_iter("") {
            let mut cursor = field
                .file_impacts(&term)
                .ok_or_else(|| failure("stored field has no posting skip runs"))?;
            cursor.advance_shallow(self.start);
            while !cursor.exhausted() && cursor.doc_id() < self.end {
                if cursor.doc_id() >= self.start {
                    self.read_posting(fi, field, &term, &cursor)?;
                }
                cursor.next_posting();
            }
        }
        Ok(())
    }

    /// Append the cursor's current posting of `term` to its row, if kept.
    fn read_posting(
        &mut self,
        fi: usize,
        field: FieldView<'_>,
        term: &str,
        cursor: &FileImpacts<'_>,
    ) -> Result<(), Status> {
        let (reader, limit) = (self.reader, self.limit);
        let original = cursor.doc_id();
        let Some(row) = &mut self.rows[(original - self.start) as usize] else {
            return Ok(());
        };
        let offsets = cursor.offsets();
        let positions = if reader.field_has_positions(fi) {
            Some(
                field
                    .posting_positions(term, original)
                    .ok_or_else(|| failure("declared positional posting has no ordinals"))?,
            )
        } else {
            None
        };
        let pos_lengths = if field.has_pos_lengths() {
            let expected = positions.as_ref().map_or(0, Vec::len);
            Some(
                field
                    .posting_pos_lengths(term, original)
                    .unwrap_or_else(|| vec![1; expected]),
            )
        } else {
            None
        };
        let bytes = term.len()
            + std::mem::size_of::<(String, u32, Vec<(u32, u32)>)>()
            + offsets.len() * 8
            + positions
                .as_ref()
                .map_or(0, |p| std::mem::size_of::<Vec<u32>>() + p.len() * 4)
            + pos_lengths
                .as_ref()
                .map_or(0, |p| std::mem::size_of::<Vec<u32>>() + p.len() * 4);
        charge(&mut self.used, bytes, limit)?;
        charge(&mut row.bytes, bytes, limit)?;
        row.fields[fi]
            .terms
            .push((term.to_owned(), cursor.tf(), offsets));
        if let Some(positions) = positions {
            row.fields[fi].positions.as_mut().unwrap().push(positions);
        }
        if let Some(pos_lengths) = pos_lengths {
            row.fields[fi]
                .pos_lengths
                .as_mut()
                .unwrap()
                .push(pos_lengths);
        }
        Ok(())
    }
}
