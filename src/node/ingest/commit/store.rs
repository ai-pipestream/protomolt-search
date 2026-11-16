//! Unified BM25 row and column insertion across `Segmented`, `Building`, and `Spilling` shards.

use std::io;

use super::columns::ValidatedColumns;
use crate::node::*;
use crate::postings::{AnalyzedDoc, DocLineage};
use crate::source_archive::SourceArchive;

/// Row sink over [`Bm25Store`]: a path-less heap store or a segmented tail.
trait Bm25RowSink {
    fn source_archive_mut(&mut self) -> &mut SourceArchive;
    fn add_row(
        &mut self,
        row_id: u32,
        text: String,
        analyzed: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> Result<(), Status>;
    fn set_facet(&mut self, fi: usize, row_id: u32, value: &str);
    fn set_numeric(&mut self, ni: usize, row_id: u32, value: f64);
    fn set_map_facet(&mut self, ci: usize, row_id: u32, key: &str, value: &str);
    fn set_map_numeric(&mut self, ci: usize, row_id: u32, key: &str, value: f64);
    fn set_map_integer(&mut self, ci: usize, row_id: u32, key: &str, value: i64) -> io::Result<()>;
    fn set_map_unsigned_integer(
        &mut self,
        ci: usize,
        row_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()>;
    fn set_integer(&mut self, ii: usize, row_id: u32, value: i64);
    fn set_unsigned_integer(&mut self, ii: usize, row_id: u32, value: u64);
    fn set_geo(&mut self, gi: usize, row_id: u32, lat: f64, lon: f64);
}

impl Bm25RowSink for Bm25Store {
    fn source_archive_mut(&mut self) -> &mut SourceArchive {
        Bm25Store::source_archive_mut(self)
    }

    fn add_row(
        &mut self,
        row_id: u32,
        text: String,
        analyzed: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> Result<(), Status> {
        self.add_document_with_lineage(row_id, text, analyzed, lineage);
        Ok(())
    }

    fn set_facet(&mut self, fi: usize, row_id: u32, value: &str) {
        Bm25Store::set_facet(self, fi, row_id, value);
    }

    fn set_numeric(&mut self, ni: usize, row_id: u32, value: f64) {
        Bm25Store::set_numeric(self, ni, row_id, value);
    }

    fn set_map_facet(&mut self, ci: usize, row_id: u32, key: &str, value: &str) {
        Bm25Store::set_map_facet(self, ci, row_id, key, value);
    }

    fn set_map_numeric(&mut self, ci: usize, row_id: u32, key: &str, value: f64) {
        Bm25Store::set_map_numeric(self, ci, row_id, key, value);
    }

    fn set_map_integer(&mut self, ci: usize, row_id: u32, key: &str, value: i64) -> io::Result<()> {
        Bm25Store::set_map_integer(self, ci, row_id, key, value)
    }

    fn set_map_unsigned_integer(
        &mut self,
        ci: usize,
        row_id: u32,
        key: &str,
        value: u64,
    ) -> io::Result<()> {
        Bm25Store::set_map_unsigned_integer(self, ci, row_id, key, value)
    }

    fn set_integer(&mut self, ii: usize, row_id: u32, value: i64) {
        Bm25Store::set_integer(self, ii, row_id, value);
    }

    fn set_unsigned_integer(&mut self, ii: usize, row_id: u32, value: u64) {
        Bm25Store::set_unsigned_integer(self, ii, row_id, value);
    }

    fn set_geo(&mut self, gi: usize, row_id: u32, lat: f64, lon: f64) {
        Bm25Store::set_geo(self, gi, row_id, lat, lon);
    }
}

/// Apply a validated document and its typed column slots to `shard`.
pub(super) fn apply_bm25_row(
    shard: &mut Bm25Shard,
    doc_id: u32,
    doc: &AddDocumentsRequest,
    analyzed: AnalyzedDoc,
    columns: &ValidatedColumns<'_>,
) -> Result<(), Status> {
    match shard {
        Bm25Shard::Segmented(segmented) => {
            let local = doc_id - segmented.tail_base();
            write_row_to_sink(segmented.tail_mut(), local, doc, analyzed, columns)?;
            segmented.sync_tail();
            Ok(())
        }
        Bm25Shard::Building(store) => write_row_to_sink(store, doc_id, doc, analyzed, columns),
    }
}

fn write_row_to_sink<S: Bm25RowSink>(
    sink: &mut S,
    row_id: u32,
    doc: &AddDocumentsRequest,
    analyzed: AnalyzedDoc,
    columns: &ValidatedColumns<'_>,
) -> Result<(), Status> {
    if let Some(source) = &doc.original_source {
        sink.source_archive_mut()
            .attach_source_with_identity(
                row_id,
                source,
                doc.source_chunk_ordinal,
                doc.identity.as_ref(),
            )
            .map_err(|e| Status::internal(format!("retain source: {e}")))?;
    }
    let lineage = doc.lineage.as_ref().map(|l| DocLineage {
        parent_id: l.parent_id,
        group_id: l.group_id,
        span_start: l.span_start,
        span_end: l.span_end,
    });
    sink.add_row(row_id, doc.text.clone(), analyzed, lineage)?;
    for (fi, value) in &columns.facets {
        sink.set_facet(*fi, row_id, value);
    }
    for &(ni, value) in &columns.numerics {
        sink.set_numeric(ni, row_id, value);
    }
    for &(ci, key, value) in &columns.map_facets {
        sink.set_map_facet(ci, row_id, key, value);
    }
    for &(ci, key, value) in &columns.map_numerics {
        sink.set_map_numeric(ci, row_id, key, value);
    }
    for &(ci, key, value) in &columns.map_integers {
        sink.set_map_integer(ci, row_id, key, value)
            .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
    }
    for &(ci, key, value) in &columns.map_unsigned_integers {
        sink.set_map_unsigned_integer(ci, row_id, key, value)
            .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
    }
    for &(ii, value) in &columns.integers {
        sink.set_integer(ii, row_id, value);
    }
    for &(ii, value) in &columns.unsigned_integers {
        sink.set_unsigned_integer(ii, row_id, value);
    }
    for &(gi, lat, lon) in &columns.geo_points {
        sink.set_geo(gi, row_id, lat, lon);
    }
    Ok(())
}
