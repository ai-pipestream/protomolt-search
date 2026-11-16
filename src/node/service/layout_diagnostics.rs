//! Shard layout diagnostics: sealed segments, summaries, tail, and knobs.

use crate::node::*;
use crate::pb::{SegmentColumnRange, SegmentDiagnostics, ShardLayoutDiagnostics};
use crate::segments::OpenedSegmentSet;

/// Mixed placement: a sealed segment whose placement column spans more than
/// one code, two segments under different codes, or a segment under a code
/// other than the pinned one. A pinned shard's tail cannot be mixed: ingest
/// refuses another code before it is applied.
fn placement_mixed(set: &OpenedSegmentSet, column: &str, pinned: Option<i64>) -> bool {
    let mut codes: Vec<i64> = Vec::new();
    let mut mixed = false;
    for i in 0..set.len() {
        let Some(summary) = set.metadata(i).summary.as_ref() else {
            continue;
        };
        let Some(range) = summary.int_columns.iter().find(|c| c.name == column) else {
            continue;
        };
        if range.present == 0 {
            continue;
        }
        if range.min != range.max {
            mixed = true;
        }
        if !codes.contains(&range.min) {
            codes.push(range.min);
        }
    }
    if codes.len() > 1 {
        mixed = true;
    }
    if let Some(pinned) = pinned {
        if codes.iter().any(|code| *code != pinned) {
            mixed = true;
        }
    }
    mixed
}

fn segment_diagnostics(set: &OpenedSegmentSet, i: usize) -> SegmentDiagnostics {
    let meta = set.metadata(i);
    let mut columns = Vec::new();
    let mut partition = None;
    if let Some(summary) = meta.summary.as_ref() {
        for c in &summary.int_columns {
            columns.push(SegmentColumnRange {
                column: c.name.clone(),
                lo: c.min,
                hi: c.max,
                present: c.present,
                ..Default::default()
            });
        }
        for c in &summary.numeric_columns {
            columns.push(SegmentColumnRange {
                column: c.name.clone(),
                lo_f: c.min,
                hi_f: c.max,
                present: c.present,
                floating: true,
                ..Default::default()
            });
        }
        partition = summary.partition.as_ref().map(|p| SegmentColumnRange {
            column: p.column.clone(),
            lo: p.lo,
            hi: p.hi,
            present: meta.live_rows,
            ..Default::default()
        });
    }
    SegmentDiagnostics {
        segment_id: meta.segment_id.clone(),
        generation: meta.generation,
        base: meta.base_label,
        rows: meta.rows,
        live_rows: meta.live_rows,
        has_summary: meta.summary.is_some(),
        columns,
        partition,
        mapped: set.vector(i).is_some_and(|v| v.is_mapped()),
    }
}

impl NodeServiceImpl {
    /// Collect layout diagnostics for `GetShardDiagnostics`: sealed segments
    /// and summaries, tail rows, tombstones, and live knob values.
    pub fn shard_diagnostics(
        &self,
        shard: u32,
        address: String,
    ) -> crate::pb::ShardLayoutDiagnostics {
        let guard = read_shard(&self.state);
        let vector_rows = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let doc_rows = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let rows = vector_rows.max(doc_rows);
        let tombstones = guard.live_docs.deleted_count();
        let scoring_fingerprint = guard
            .index
            .as_ref()
            .map(|i| i.descriptor().scoring_fingerprint)
            .unwrap_or_default();
        let mut out = ShardLayoutDiagnostics {
            shard,
            address,
            layout: match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(_)) => "segments".to_string(),
                Some(_) => "single-image".to_string(),
                None => match self.config.layout {
                    Layout::Segments => "segments".to_string(),
                    Layout::SingleImage => "single-image".to_string(),
                },
            },
            rows,
            live_rows: rows.saturating_sub(tombstones),
            tombstones,
            scoring_fingerprint,
            segment_pruning: self.knobs.segment_pruning(),
            floor_sharing: self.knobs.share_floors(),
            has_placement: self.config.placement_leaf.is_some(),
            placement: self.config.placement_leaf.unwrap_or_default() as u64,
            ..Default::default()
        };
        if let Some(Bm25Shard::Segmented(segmented)) = guard.bm25.as_ref() {
            let set = segmented.snapshot();
            if let Some(column) = self.config.placement_column.as_deref() {
                out.placement_mixed = placement_mixed(set, column, self.config.placement_leaf);
            }
            out.catalog_epoch = set.epoch();
            out.partition_key = set.manifest().partition_key.clone().unwrap_or_default();
            out.tail_rows = u64::from(
                segmented
                    .next_doc_id()
                    .saturating_sub(segmented.tail_base()),
            );
            for i in 0..set.len() {
                out.segments.push(segment_diagnostics(set, i));
            }
        }
        out
    }
}
