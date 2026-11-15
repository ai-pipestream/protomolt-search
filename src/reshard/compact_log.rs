//! Compaction of one shard's WAL into a fresh image.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::pb::AddDocumentsRequest;
use crate::vector::VectorIndex;
use crate::wal::WalManifest;

use super::child_build::{child_order, finish_child_ordered, ChildSchema, ChildSlot};
#[cfg(doc)]
use super::hash_split::split_logs_with;
use super::hnsw_sources::HnswCompactionSources;
use super::replay::{
    read_gen_manifest, read_gens_binding, replay_buckets_through, require_backend_config,
    require_complete_history, Replay,
};
#[cfg(doc)]
use super::segmented_split::split_logs_segmented_with;
use super::{Analyzer, ChildImage, ColumnTables};

/// One live row of a compaction's dense image, in the order the sink
/// receives them: new local slot order, which is also the order the
/// rewritten log records them in.
pub struct CompactedRow<'a> {
    /// The row's local slot in the dense image (the node adds its slot
    /// offset for the global id).
    pub new_local: u64,
    /// The global id the row had in the source generation.
    pub old_id: u64,
    pub vector: Option<&'a [f32]>,
    /// Validated materialized retention for this vector, with original bits.
    pub materialized_row: Option<&'a [u8]>,
    pub document: Option<&'a AddDocumentsRequest>,
    pub stable_key: Option<&'a [u8]>,
}

/// Where a compaction's live rows go besides the image: the rewritten
/// full-history WAL generation. Called once per row
/// in slot order, before the image is built from the same replay.
pub type RowSink<'a> = dyn FnMut(CompactedRow<'_>) -> Result<(), String> + 'a;

/// What [`compact_log`] built: the dense image(s), the id map, and the
/// counts the operator asked for.
#[derive(Debug)]
pub struct CompactionBuild {
    /// The rewritten generation's number: one past the source.
    pub generation: u64,
    /// Rows the source generation held through the cutoff, tombstoned or
    /// not.
    pub rows_before: u64,
    /// Rows the cutoff had tombstoned, which the dense image drops.
    pub tombstones: u64,
    /// Source global id -> dense LOCAL slot, for every live row.
    pub id_map: BTreeMap<u64, u64>,
    /// The dense image(s): one for the single-image layout; one per
    /// non-empty WAL bucket for the segment layout, each with its
    /// `slot_offset` set to its local base label.
    pub images: Vec<ChildImage>,
    /// The mapped-plan binding the source carried, to be logged first in
    /// the rewritten generation.
    pub binding: Option<crate::postings::StoredBinding>,
}

/// Hand every live row of `replay` to `sink` in explicit dense slot `order`,
/// extending `id_map` with `old_id -> new_local`. Returns the number of rows
/// emitted.
pub(super) fn emit_rows_in(
    replay: &Replay,
    order: &[u64],
    slot_base: u64,
    id_map: &mut BTreeMap<u64, u64>,
    sink: &mut RowSink,
) -> Result<u64, String> {
    for (slot, old_id) in order.iter().enumerate() {
        let new_local = slot_base + slot as u64;
        if id_map.insert(*old_id, new_local).is_some() {
            return Err(format!(
                "compaction saw source id {old_id} twice; the log is corrupt"
            ));
        }
        sink(CompactedRow {
            new_local,
            old_id: *old_id,
            vector: replay.vectors.get(old_id).map(Vec::as_slice),
            materialized_row: replay.retained.get(old_id).map(Vec::as_slice),
            document: replay.documents.get(old_id),
            stable_key: replay.stable_keys.get(old_id).map(Vec::as_slice),
        })?;
    }
    Ok(order.len() as u64)
}

/// Rows a replay holds before its tombstones are applied, and how many of
/// them are tombstoned: `(present, tombstoned)`.
pub(super) fn replay_counts(replay: &Replay) -> (u64, u64) {
    let present: BTreeSet<&u64> = replay
        .vectors
        .keys()
        .chain(replay.documents.keys())
        .collect();
    let tombstoned = replay
        .deleted
        .iter()
        .filter(|id| present.contains(id))
        .count() as u64;
    (present.len() as u64, tombstoned)
}

/// When `manifest`'s backend is `hnsw` with a non-`None` `locality_order` and
/// `replay` holds at least `MIN_GRAPH_ROWS` vectors, build/merge the HNSW
/// graph up front, permute it globally in-place (`0 B/row` overhead, no node
/// map), and return the matching segment-wide slot order (`#98`).
fn prepare_compacted_replay_order(
    manifest: &WalManifest,
    replay: &Replay,
    hnsw_sources: Option<&HnswCompactionSources>,
) -> Result<(Vec<u64>, Option<VectorIndex>), String> {
    let default_order = child_order(replay);
    if replay.vectors.len() < crate::hnsw::MIN_GRAPH_ROWS {
        return Ok((default_order, None));
    }
    let backend_config = manifest
        .backend_config()
        .map_err(|e| format!("construct child backend config: {e}"))?;
    if backend_config.backend_kind != crate::hnsw::HNSW {
        return Ok((default_order, None));
    }
    let config = crate::hnsw::HnswConfig::from_backend_config(&backend_config)
        .map_err(|e| format!("decode child hnsw config: {e}"))?;
    if config.locality_order == crate::hnsw::LocalityOrder::None {
        return Ok((default_order, None));
    }
    let dim = manifest.dim as usize;
    let vec_pairs: Vec<(u64, Vec<f32>)> = replay
        .vectors
        .iter()
        .map(|(&id, v)| (id, v.clone()))
        .collect();
    let merged = match hnsw_sources {
        Some(sources) => sources.build_merged_hnsw(dim, &backend_config, &vec_pairs)?,
        None => None,
    };
    let mut hnsw = match merged {
        Some(index) => index,
        None => {
            use crate::vector::VectorProvider;
            let mut flat = Vec::with_capacity(vec_pairs.len() * dim);
            for (_, v) in &vec_pairs {
                flat.extend_from_slice(v);
            }
            let mut index = crate::hnsw::HnswIndex::new(dim, config)
                .map_err(|e| format!("construct child hnsw index: {e}"))?;
            index
                .add(&flat, dim)
                .map_err(|e| format!("add child hnsw vectors: {e}"))?;
            index
        }
    };
    let perm = hnsw
        .reorder_global_in_place()
        .map_err(|e| format!("reorder child hnsw index: {e}"))?;
    let order = match perm {
        Some(new_to_old) => {
            let mut out = Vec::with_capacity(default_order.len());
            for old_idx in new_to_old {
                out.push(vec_pairs[old_idx as usize].0);
            }
            out.extend(
                replay
                    .documents
                    .keys()
                    .filter(|id| !replay.vectors.contains_key(id))
                    .copied(),
            );
            out
        }
        None => default_order,
    };
    Ok((order, Some(hnsw.into_index())))
}

/// One compaction's source generation and how its outputs are written:
/// the generation, the clock its dense image stops at, the output
/// directory, and the tables every output declares.
#[derive(Clone, Copy)]
pub struct CompactionRequest<'a> {
    pub gen: &'a Path,
    pub cutoff_clock: u64,
    pub out_dir: &'a Path,
    /// The BM25 field table; derived from the records when `None`.
    pub bm25_fields: Option<&'a [String]>,
    /// The source shard's per-field analyzer fingerprints, pinned on
    /// every output.
    pub pinned_fingerprints: Option<&'a [u64]>,
    /// The shard's column tables, so an output that holds no record of a
    /// declared column still declares it.
    pub columns: Option<&'a ColumnTables>,
    pub derived: Option<&'a crate::postings::StoredDerived>,
    pub(crate) hnsw_sources: Option<&'a HnswCompactionSources>,
}

impl<'a> CompactionRequest<'a> {
    /// A request with every table derived from the records.
    pub fn new(gen: &'a Path, cutoff_clock: u64, out_dir: &'a Path) -> Self {
        Self {
            gen,
            cutoff_clock,
            out_dir,
            bm25_fields: None,
            pinned_fingerprints: None,
            columns: None,
            derived: None,
            hnsw_sources: None,
        }
    }

    pub(super) fn schema(
        &self,
        binding: Option<&'a crate::postings::StoredBinding>,
    ) -> ChildSchema<'a> {
        ChildSchema {
            bm25_fields: self.bm25_fields,
            pinned_fingerprints: self.pinned_fingerprints,
            binding,
            derived: self.derived,
            columns: self.columns,
            hnsw_sources: self.hnsw_sources,
        }
    }
}

/// The source generation of a compaction, checked and opened: its
/// manifest, its binding, and the generation number the output takes.
pub(super) struct CompactionSource<'a> {
    pub(super) request: &'a CompactionRequest<'a>,
    pub(super) manifest: WalManifest,
    pub(super) binding: Option<crate::postings::StoredBinding>,
    pub(super) generation: u64,
}

impl<'a> CompactionSource<'a> {
    /// The generation must be full history with a usable provider
    /// configuration, exactly as for a reshard; those checks refuse here
    /// before anything is read.
    pub(super) fn open(request: &'a CompactionRequest<'a>) -> Result<Self, String> {
        let gen = request.gen;
        let manifest = read_gen_manifest(gen)?;
        require_backend_config(&manifest, gen)?;
        require_complete_history(&manifest, gen)?;
        let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
        std::fs::create_dir_all(request.out_dir)
            .map_err(|error| format!("mkdir {}: {error}", request.out_dir.display()))?;
        let generation = manifest
            .generation
            .checked_add(1)
            .ok_or_else(|| "compaction generation overflow".to_string())?;
        Ok(Self {
            request,
            manifest,
            binding,
            generation,
        })
    }

    /// The live and tombstoned rows of `buckets` through the cutoff.
    pub(super) fn replay(&self, buckets: std::ops::Range<u32>) -> Result<Replay, String> {
        let mut replay = Replay::default();
        replay_buckets_through(
            self.request.gen,
            buckets,
            self.manifest.bucket_count as usize,
            self.manifest.dim as usize,
            false,
            self.request.cutoff_clock,
            &mut replay,
        )?;
        Ok(replay)
    }

    pub(super) fn schema(&self) -> ChildSchema<'_> {
        self.request.schema(self.binding.as_ref())
    }

    /// The build result from its parts, with the source's binding.
    pub(super) fn build(self, counts: RowCounts, images: Vec<ChildImage>) -> CompactionBuild {
        CompactionBuild {
            generation: self.generation,
            rows_before: counts.rows_before,
            tombstones: counts.tombstones,
            id_map: counts.id_map,
            images,
            binding: self.binding,
        }
    }
}

/// What a compaction counts as it replays: rows before tombstones, the
/// tombstoned ones, and every live row's new local slot.
#[derive(Default)]
pub(super) struct RowCounts {
    pub(super) rows_before: u64,
    pub(super) tombstones: u64,
    pub(super) id_map: BTreeMap<u64, u64>,
}

impl RowCounts {
    pub(super) fn count(&mut self, replay: &Replay) {
        let (present, tombstoned) = replay_counts(replay);
        self.rows_before += present;
        self.tombstones += tombstoned;
    }
}

/// Build the dense all-live image of ONE generation through `cutoff_clock`, the
/// baseline of an online compaction: the log prefix is replayed with its deletes
/// and replacements applied, every live row is handed to `sink` in dense slot order
/// (the rewritten log), and the image is written under `out_dir`. Local slots start
/// at 0; the caller owns the global offset.
///
/// `segmented` selects the bucket-bounded shape: one sealed image per non-empty WAL
/// bucket, slots dense across them in bucket order, memory holding one bucket's
/// replay at a time — the same shape [`split_logs_segmented_with`] gives a catalog.
/// Otherwise one image over the whole replay, as [`split_logs_with`] with a factor
/// of one.
pub fn compact_log(
    request: &CompactionRequest<'_>,
    segmented: bool,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let source = CompactionSource::open(request)?;
    let mut counts = RowCounts::default();
    let images = if segmented {
        compact_buckets(&source, &mut counts, analyze, sink)?
    } else {
        let mut replay = source.replay(0..source.manifest.bucket_count)?;
        counts.count(&replay);
        replay.compact();
        let (image, _) = compact_image(&source, replay, 0, 0, &mut counts, analyze, sink)?;
        vec![image]
    };
    Ok(source.build(counts, images))
}

/// One image per non-empty WAL bucket, slots dense across them.
fn compact_buckets(
    source: &CompactionSource<'_>,
    counts: &mut RowCounts,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<Vec<ChildImage>, String> {
    let mut images = Vec::new();
    let mut next_slot = 0u64;
    for bucket in 0..source.manifest.bucket_count {
        let mut replay = source.replay(bucket..bucket + 1)?;
        counts.count(&replay);
        replay.compact();
        if replay.vectors.is_empty() && replay.documents.is_empty() {
            continue;
        }
        let ordinal = images.len();
        let (image, rows) =
            compact_image(source, replay, ordinal, next_slot, counts, analyze, sink)?;
        images.push(image);
        next_slot = next_slot
            .checked_add(rows)
            .ok_or_else(|| "compaction slot overflow".to_string())?;
    }
    Ok(images)
}

/// The rows of one compacted replay: handed to the sink in slot order
/// from `next_slot`, then built into image `ordinal`. Returns the image
/// and the rows it holds.
fn compact_image(
    source: &CompactionSource<'_>,
    replay: Replay,
    ordinal: usize,
    next_slot: u64,
    counts: &mut RowCounts,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<(ChildImage, u64), String> {
    let schema = source.schema();
    let (order, prebuilt) =
        prepare_compacted_replay_order(&source.manifest, &replay, schema.hnsw_sources)?;
    let rows = emit_rows_in(&replay, &order, next_slot, &mut counts.id_map, sink)?;
    let slot = ChildSlot::full_range(ordinal, source.request.out_dir, next_slot);
    let image = finish_child_ordered(
        &source.manifest,
        replay,
        &order,
        slot,
        &schema,
        prebuilt,
        analyze,
    )?;
    Ok((image, rows))
}
