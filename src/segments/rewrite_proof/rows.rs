//! Digest every live row of a segment set into the proof's scratch table.

use super::*;
use crate::postings::FieldView;

fn identity(reader: &Bm25Reader, row: u32) -> Result<Vec<u8>, String> {
    let identity = reader
        .document_identity(row)
        .ok_or_else(|| err("live row has no document identity"))?;
    if identity.document_key.is_empty()
        || identity.document_key.len() > 16384
        || identity.version == 0
    {
        return Err(err("live row has an invalid document identity"));
    }
    Ok(identity.encode_to_vec())
}

/// Rows `start..end` of one segment. A batch's digests are indexed from
/// `start`.
struct Batch<'a> {
    set: &'a OpenedSegmentSet,
    segment: usize,
    reader: &'a Bm25Reader,
    tables: &'a SourceTables,
    live: &'a LiveDocs,
    start: usize,
    end: usize,
}

pub(super) fn fill(
    database: &Database,
    table: TableDefinition<&[u8], &[u8]>,
    set: &OpenedSegmentSet,
    batch_rows: usize,
) -> Result<u64, String> {
    let tx = database.begin_write().map_err(err)?;
    tx.open_table(table).map_err(err)?;
    tx.commit().map_err(err)?;
    let mut count = 0u64;
    for segment in 0..set.len() {
        let reader = set.bm25(segment);
        let tables = segment_tables(reader);
        let rows = usize::try_from(set.metadata(segment).rows).map_err(err)?;
        u32::try_from(rows).map_err(|_| err("segment row count exceeds stored document ids"))?;
        for start in (0..rows).step_by(batch_rows) {
            let batch = Batch {
                set,
                segment,
                reader,
                tables: &tables,
                live: set.live_docs(segment),
                start,
                end: rows.min(start + batch_rows),
            };
            let mut digests = vec![[0u8; 32]; batch.end - start];
            batch.digest_rows(&mut digests)?;
            for fi in 0..reader.field_count() {
                #[cfg(test)]
                VOCABULARY_PASSES.with(|passes| passes.set(passes.get() + 1));
                batch.fold_field(fi, &mut digests)?;
            }
            count = batch.store(database, table, &digests, count)?;
        }
    }
    Ok(count)
}

impl Batch<'_> {
    /// The digest of one row's stored content, exact vector and, when the
    /// segment has a provider, that provider's `transcript` of the row.
    fn digest_row(&self, row: usize, transcript: Option<&[u8]>) -> Result<[u8; 32], String> {
        let doc = reconstruct_document(self.reader, row as u32, self.tables, true)?;
        if doc.original_source.is_none() {
            return Err(err("live source row has no original protobuf"));
        }
        let mut hash = Sha256::new();
        part(&mut hash, b"protomolt.source-rewrite.row.v3");
        part(&mut hash, &doc.encode_to_vec());
        // Protobuf scalar default elision normalizes -0. Preserve its
        // stored bits too, just as the FP32 vector transcript does.
        for value in &doc.numerics {
            hash.update(&value.value.to_bits().to_le_bytes());
        }
        for value in &doc.map_numerics {
            hash.update(&value.value.to_bits().to_le_bytes());
        }
        for value in &doc.geo_points {
            hash.update(&value.lat.to_bits().to_le_bytes());
            hash.update(&value.lon.to_bits().to_le_bytes());
        }
        let retained = self.set.retained_segment(self.segment);
        if let Some(retained) = retained.retained(row as u32) {
            part(&mut hash, b"protomolt.source-rewrite.retained-vector.v1");
            part(&mut hash, &crate::multi_res::encode(retained).map_err(err)?);
        }
        if let Some(exact) = self.set.exact_vectors(self.segment) {
            hash.update(&[1]);
            for value in exact.row_values(row, row + 1).map_err(err)? {
                hash.update(&value.to_bits().to_le_bytes());
            }
        } else {
            hash.update(&[0]);
        }
        // The provider's stored encoding of the row, not a recomputation
        // from the FP32 source: two images that agree on every exact row
        // and differ in what their scorer reads are different indexes.
        match transcript {
            Some(transcript) => {
                hash.update(&[1]);
                part(&mut hash, transcript);
            }
            None => {
                hash.update(&[0]);
            }
        }
        Ok(hash.finish().0)
    }

    /// Start every live row's digest from its stored content.
    fn digest_rows(&self, digests: &mut [[u8; 32]]) -> Result<(), String> {
        let (start, end) = (self.start, self.end);
        let Some(vector) = self.set.vector(self.segment) else {
            for row in start..end {
                if self.live.is_deleted(row) {
                    continue;
                }
                digests[row - start] = self.digest_row(row, None)?;
            }
            return Ok(());
        };
        // Transcripts arrive in pieces of TRANSCRIPT_BLOCK_ROWS: the
        // provider converts its own layout one block at a time and
        // keeps no materialized copy, so a large image is read
        // inside the batch's budget.
        let mut block = start;
        while block < end {
            let block_end = end.min(block + crate::vector::TRANSCRIPT_BLOCK_ROWS);
            let mut failure = None;
            vector
                .row_transcripts(block..block_end, &mut |row, transcript| {
                    if failure.is_some() || self.live.is_deleted(row) {
                        return;
                    }
                    match self.digest_row(row, Some(transcript)) {
                        Ok(digest) => digests[row - start] = digest,
                        Err(error) => failure = Some(error),
                    }
                })
                .map_err(err)?;
            if let Some(error) = failure {
                return Err(error);
            }
            block = block_end;
        }
        Ok(())
    }

    /// Extend every live row's digest with what field `fi` stores for it.
    fn fold_field(&self, fi: usize, digests: &mut [[u8; 32]]) -> Result<(), String> {
        let (start, end) = (self.start, self.end);
        let field = self.reader.field(fi);
        for row in start..end {
            if self.live.is_deleted(row) {
                continue;
            }
            let mut hash = extend(&digests[row - start], b"field");
            hash.update(&(fi as u64).to_le_bytes());
            hash.update(&field.doc_length(row as u32).to_le_bytes());
            hash.update(&[u8::from(self.reader.field_has_positions(fi))]);
            let sentences = self.reader.field_doc_sentences(fi, row as u32);
            hash.update(&[u8::from(sentences.is_some())]);
            if let Some(sentences) = sentences {
                hash.update(&(sentences.len() as u64).to_le_bytes());
                for (start, end) in sentences {
                    hash.update(&start.to_le_bytes());
                    hash.update(&end.to_le_bytes());
                }
            }
            digests[row - start] = hash.finish().0;
        }
        field.visit_splade(start as u32, end as u32, |term, row, impact, scale| {
            let row = row as usize;
            if !self.live.is_deleted(row) {
                let mut hash = extend(&digests[row - start], b"splade");
                part(&mut hash, term.as_bytes());
                hash.update(&impact.to_le_bytes());
                hash.update(&scale.to_le_bytes());
                digests[row - start] = hash.finish().0;
            }
            Ok::<(), String>(())
        })?;
        for term in field.prefix_iter("") {
            self.fold_postings(&field, &term, digests)?;
        }
        Ok(())
    }

    /// Extend the digest of every live row in the batch that holds `term`.
    fn fold_postings(
        &self,
        field: &FieldView<'_>,
        term: &str,
        digests: &mut [[u8; 32]],
    ) -> Result<(), String> {
        let (start, end) = (self.start, self.end);
        let mut cursor = field
            .file_impacts(term)
            .ok_or_else(|| err("rewrite proof requires posting skip runs"))?;
        cursor.advance_shallow(start as u32);
        while !cursor.exhausted() && (cursor.doc_id() as usize) < end {
            let row = cursor.doc_id() as usize;
            if row >= start && !self.live.is_deleted(row) {
                let mut hash = extend(&digests[row - start], b"posting");
                part(&mut hash, term.as_bytes());
                hash.update(&cursor.tf().to_le_bytes());
                let offsets = cursor.offsets();
                hash.update(&(offsets.len() as u64).to_le_bytes());
                for (start, end) in offsets {
                    hash.update(&start.to_le_bytes());
                    hash.update(&end.to_le_bytes());
                }
                let positions = field.posting_positions(term, row as u32);
                hash.update(&[u8::from(positions.is_some())]);
                if let Some(positions) = positions {
                    hash.update(&(positions.len() as u64).to_le_bytes());
                    for pos in positions {
                        hash.update(&pos.to_le_bytes());
                    }
                }
                // Absence means unit arcs, including legacy segments merged
                // with lattice-bearing rows in the same destination field.
                if let Some(positions) = field.posting_positions(term, row as u32) {
                    let lengths = field
                        .posting_pos_lengths(term, row as u32)
                        .unwrap_or_else(|| vec![1; positions.len()]);
                    for len in lengths {
                        hash.update(&len.to_le_bytes());
                    }
                }
                digests[row - start] = hash.finish().0;
            }
            cursor.next_posting();
        }
        Ok(())
    }

    /// Insert every live row's digest under its identity; the running
    /// live-row count after this batch.
    fn store(
        &self,
        database: &Database,
        table: TableDefinition<&[u8], &[u8]>,
        digests: &[[u8; 32]],
        mut count: u64,
    ) -> Result<u64, String> {
        let mut tx = database.begin_write().map_err(err)?;
        tx.set_durability(Durability::None).map_err(err)?;
        {
            let mut output = tx.open_table(table).map_err(err)?;
            for row in self.start..self.end {
                if self.live.is_deleted(row) {
                    continue;
                }
                let key = identity(self.reader, row as u32)?;
                if output
                    .insert(key.as_slice(), digests[row - self.start].as_slice())
                    .map_err(err)?
                    .is_some()
                {
                    return Err(err("duplicate live document/chunk identity"));
                }
                count = count
                    .checked_add(1)
                    .ok_or_else(|| err("live row count overflow"))?;
            }
        }
        tx.commit().map_err(err)?;
        Ok(count)
    }
}
