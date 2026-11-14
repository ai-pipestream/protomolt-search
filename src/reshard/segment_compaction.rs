//! Partitioned compaction straight from sealed segments.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::analyzer::SessionLayers;
use crate::pb::wal::wal_record;
use crate::pb::AnalysisSpec;
use crate::postings::{AnalyzedDoc, AnalyzedField};
use crate::wal::{self, WalManifest};

use super::analysis_sidecar::{analysis_sidecar_path, read_analysis_file, write_analysis_entry};
use super::child_build::finish_child_ordered_with_sources;
use super::hnsw_sources::HnswCompactionSources;
use super::partitioned::{partition_key_of, ChildCutPlan};
use super::replay::{replay_buckets_routed, Replay};
use super::segment_sources::{reconstruct_document, segment_tables};
use super::tree_split::{live_rows, BuiltBuckets, BUILD_BYTES_PER_ROW};
use super::{Analyzer, ChildImage, PartitionSpec};

// ---------------------------------------------------------------------
// Compaction from segments (docs/replay-from-segments.md, "Partitioned
// compaction of a catalog without a log")
// ---------------------------------------------------------------------

/// The build bounds a compaction from segments accepts: the same knobs a
/// re-placement split's segmented build takes (`--build-threads`,
/// `--build-queue`, `--build-memory`, docs/replay-from-segments.md),
/// with the same refusal rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentCompactionOptions {
    /// Cuts built at once: each thread replays one cut's spill into
    /// memory and seals it, the images append in cut order, so the
    /// catalog is the one-thread build's, byte for byte.
    pub build_threads: usize,
    /// Sealed-but-unappended cut images held at once: a worker waits for
    /// backlog space before claiming its next cut. `None` takes the
    /// thread count; zero is refused.
    pub build_queue: Option<usize>,
    /// A fixed 70 KiB/row planning admission estimate in bytes
    /// ([`BUILD_BYTES_PER_ROW`]): the largest cut alone, and the thread
    /// count times it, must fit. A plan that cannot fit is refused by
    /// name; nothing is lowered to fit. `None` enforces no check.
    pub build_memory: Option<u64>,
}

impl Default for SegmentCompactionOptions {
    fn default() -> Self {
        Self {
            build_threads: 1,
            build_queue: None,
            build_memory: None,
        }
    }
}

/// What [`compact_segments_partitioned`] built of a catalog's sealed set.
#[derive(Debug)]
pub struct SegmentCompactionBuild {
    /// One image per cut, in cut order (ascending keys, the unkeyed cut
    /// last), each `slot_offset` its dense local base.
    pub images: Vec<ChildImage>,
    /// Source global id -> dense LOCAL slot, for every live row.
    pub id_map: BTreeMap<u64, u64>,
    /// Rows the sealed set held, tombstoned or not.
    pub rows_before: u64,
    /// Rows tombstoned at the cutoff, which the outputs drop.
    pub tombstones: u64,
    /// The catalog's mapped-plan binding, pinned on every output.
    pub binding: Option<crate::postings::StoredBinding>,
}

/// Build the dense partitioned images of one catalog's sealed set without
/// its log (`docs/replay-from-segments.md`): the row source is the
/// segments themselves, each document's analyzed fields transposed from
/// the postings (`FieldView::transpose`), never the analyzer; the live
/// rows are keyed by `spec.column` and cut into partitions of at most
/// `spec.bound` rows by the split's cut-point selection ([`ChildCutPlan`],
/// the `--cut-column/--cut-rows` logic); rows without the column seal in
/// one final unkeyed cut.
///
/// `set` is the cutoff snapshot of the live catalog and `overlay` the
/// shard-wide live-document overlay at the cutoff: a row either names is
/// tombstoned and does not move. Every segment must share one field
/// table, one fingerprint per field, one column table, one derived-column
/// declaration, and one vector provider state; a difference is refused
/// naming the segment. The outputs pin those tables and the declaration
/// (the kind-15 entry) on every image, and `binding` is the catalog's own
/// generation binding.
///
/// Three passes, none holding more than one segment's transposes or one
/// cut's replay in memory: a key count over the column tables, a spill of
/// each live row into its cut's single-bucket log plus analysis sidecar,
/// then one image per cut — single-threaded, or `options.build_threads`
/// cuts at once under the same backlog and budget rules as the split.
/// Slots come from the spill's own row counts before any cut is replayed,
/// so the threaded build appends in cut order and is byte for byte the
/// serial build's.
pub fn compact_segments_partitioned(
    set: &crate::segments::OpenedSegmentSet,
    overlay: &crate::live_docs::LiveDocs,
    slot_offset: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    options: &SegmentCompactionOptions,
) -> Result<SegmentCompactionBuild, String> {
    if set.is_empty() {
        return Err(
            "a compaction from segments needs a catalog with at least one sealed \
                    segment; an empty or never-flushed catalog has no rows to transplant"
                .to_string(),
        );
    }
    let binding = set.binding().cloned().ok_or_else(|| {
        format!(
            "{}: the catalog carries no generation binding; a compaction from segments needs \
             the bound generation it rebuilds",
            set.root().display()
        )
    })?;
    if spec.bound == 0 {
        return Err("a partitioned compaction needs a positive row bound".to_string());
    }
    if options.build_queue == Some(0) {
        return Err("build queue 0 holds no finished cut; give it one or more".to_string());
    }
    // One store: every segment declares the same tables, fingerprints,
    // and declaration, or the set cannot compact into one catalog.
    let tables = segment_tables(set.bm25(0));
    let derived = set.bm25(0).derived().cloned();
    let mut rows_total = 0u64;
    for i in 0..set.len() {
        let meta = set.metadata(i);
        if meta.base_label != rows_total {
            return Err(format!(
                "{} segment {} starts at label {} but the segments before it end at \
                 {rows_total}; a compaction from segments needs one contiguous id space",
                set.root().display(),
                meta.segment_id,
                meta.base_label
            ));
        }
        rows_total = meta.end_label_exclusive()?;
        let bm25 = set.bm25(i);
        if u64::from(bm25.next_doc_id()) != meta.rows {
            return Err(format!(
                "{} segment {}: the store has {} rows, the metadata {}",
                set.root().display(),
                meta.segment_id,
                bm25.next_doc_id(),
                meta.rows
            ));
        }
        let this = segment_tables(bm25);
        if this != tables {
            return Err(format!(
                "{} segment {}: field or column tables differ from the first segment's; one \
                 catalog compacts under one table",
                set.root().display(),
                meta.segment_id
            ));
        }
        if bm25.derived() != derived.as_ref() {
            return Err(format!(
                "{} segment {}: derived-column declaration {:?} differs from the first \
                 segment's {:?}; a compaction cannot restamp a declaration",
                set.root().display(),
                meta.segment_id,
                bm25.derived().map(|d| &d.fingerprint),
                derived.as_ref().map(|d| &d.fingerprint)
            ));
        }
    }
    // One provider state: the segments' own vector images carry it, and
    // the outputs must reproduce it exactly.
    let mut provider: Option<(usize, crate::vector::VectorBackendConfig)> = None;
    for i in 0..set.len() {
        match set.vector(i) {
            Some(image) => {
                let dim = image.dim_opt().ok_or_else(|| {
                    format!(
                        "{} segment {}: vector image has no dimension",
                        set.root().display(),
                        set.metadata(i).segment_id
                    )
                })?;
                let config = image.backend_config().map_err(|e| {
                    format!(
                        "{} segment {}: {e}",
                        set.root().display(),
                        set.metadata(i).segment_id
                    )
                })?;
                match &provider {
                    Some((held_dim, held)) if *held_dim != dim || *held != config => {
                        return Err(format!(
                            "{} segment {}: vector provider state differs from the first \
                             segment's; a compaction cannot mix scoring configurations",
                            set.root().display(),
                            set.metadata(i).segment_id
                        ));
                    }
                    Some(_) => {}
                    None => provider = Some((dim, config)),
                }
                if set.exact_vectors(i).is_none() {
                    return Err(format!(
                        "{} segment {}: has a vector image but no exact-vector sidecar; a \
                         transplant copies FP32 rows, rebuild or backfill the source",
                        set.root().display(),
                        set.metadata(i).segment_id
                    ));
                }
            }
            None => {
                return Err(format!(
                    "{} segment {}: no vector image; a compaction from segments needs the \
                     provider state the sealed images carry",
                    set.root().display(),
                    set.metadata(i).segment_id
                ));
            }
        }
    }
    let (dim, backend_config) = provider.expect("checked above: the set is not empty");
    let column = spec.column;
    let column_index = tables
        .columns
        .integers
        .iter()
        .position(|name| name == column)
        .ok_or_else(|| {
            format!(
                "partition column {column:?} is not an integer column of the catalog ({:?})",
                tables.columns.integers
            )
        })?;
    let live_at = |segment: usize, row: u32| {
        let meta = set.metadata(segment);
        let label = meta.base_label + u64::from(row);
        !set.live_docs(segment).is_deleted(row as usize)
            && !overlay.is_deleted(usize::try_from(label).unwrap_or(usize::MAX))
    };

    // Pass 1: keys. The column tables only; no transpose is needed.
    let mut histogram: BTreeMap<i64, u64> = BTreeMap::new();
    let mut unkeyed = 0u64;
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    for i in 0..set.len() {
        let bm25 = set.bm25(i);
        for row in 0..bm25.next_doc_id() {
            rows_before += 1;
            if !live_at(i, row) {
                tombstones += 1;
                continue;
            }
            match bm25.integer_value(column_index, row) {
                Some(key) => *histogram.entry(key).or_insert(0) += 1,
                None => unkeyed += 1,
            }
        }
    }
    if histogram.is_empty() {
        return Err(format!(
            "partition column {column:?}: no live document of this catalog carries it"
        ));
    }
    let mut plan = ChildCutPlan::plan(&histogram, unkeyed, spec.bound as u64);
    if plan.buckets > 65_536 {
        return Err(format!(
            "the cut needs {} partitions, above 65,536; raise the row bound",
            plan.buckets
        ));
    }
    drop(histogram);

    // The spill's shape: one single-bucket log per cut, with the
    // documents' analyzed fields in an analysis sidecar beside it,
    // exactly the split's spill (docs/replay-from-segments.md).
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let mut manifest = WalManifest {
        dim: dim as u32,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        collection: String::new(),
        slot_offset,
        generation: 0,
        bucket_bits: 0,
        bucket_count: 1,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: wal::FORMAT_VERSION,
        derived_fingerprint: String::new(),
        derived: Vec::new(),
        columns: None,
    };
    manifest.set_backend_config(backend_config);
    // The spill's manifest records what its rows are derived under, as
    // the split's spill does.
    if let Some(derived) = &derived {
        manifest.derived_fingerprint = derived.fingerprint.clone();
        manifest.derived = crate::derived::config_from_spec(&crate::derived::decode_canonical(
            &derived.declaration,
        )?);
    }
    let spill_root = out_dir.join("spill");
    let mut spills = Vec::with_capacity(plan.buckets as usize);
    for bucket in 0..plan.buckets {
        let dir = spill_root.join(format!("p{bucket:05}"));
        let writer = wal::WalWriter::create(&dir, manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    let mut sidecars: Vec<Option<std::io::BufWriter<std::fs::File>>> =
        (0..plan.buckets).map(|_| None).collect();
    let mut bucket_rows = vec![0u64; plan.buckets as usize];
    for i in 0..set.len() {
        let meta = set.metadata(i);
        let bm25 = set.bm25(i);
        let exact = set.exact_vectors(i).map(std::sync::Arc::clone);
        let mut transposes = Vec::with_capacity(bm25.field_count());
        for f in 0..bm25.field_count() {
            transposes.push(bm25.field(f).transpose().map_err(|e| {
                format!("{} segment {}: {e}", set.root().display(), meta.segment_id)
            })?);
        }
        for row in 0..bm25.next_doc_id() {
            if !live_at(i, row) {
                continue;
            }
            let id = slot_offset
                .checked_add(meta.base_label)
                .and_then(|id| id.checked_add(u64::from(row)))
                .ok_or_else(|| "compaction from segments: id overflow".to_string())?;
            let document = reconstruct_document(bm25, row, &tables, true).map_err(|e| {
                format!("{} segment {}: {e}", set.root().display(), meta.segment_id)
            })?;
            let bucket = plan.bucket(partition_key_of(&document, column)?, spec.bound as u64);
            bucket_rows[bucket as usize] += 1;
            spills[bucket as usize]
                .append(wal_record::Op::AddDocuments(
                    crate::pb::wal::LoggedAddDocuments {
                        source_references: Vec::new(),
                        first_id: id,
                        documents: vec![document],
                        stable_routing_keys: Vec::new(),
                    },
                ))
                .map_err(|error| format!("spill document {id}: {error}"))?;
            let store = exact
                .as_ref()
                .expect("checked above: every segment has exact rows");
            let vector = store
                .row_values(row as usize, row as usize + 1)
                .map_err(|e| format!("read exact row {row}: {e}"))?;
            spills[bucket as usize]
                .append(wal_record::Op::AddVectors(
                    crate::pb::wal::LoggedAddVectors {
                        first_id: id,
                        batch: Some(crate::pb::AddVectorsRequest {
                            materialized_rows: set
                                .retained_segment(i)
                                .retained(row)
                                .map(crate::multi_res::encode)
                                .transpose()
                                .map_err(|error| format!("retain segment row {row}: {error}"))?
                                .into_iter()
                                .collect(),
                            vectors: vector,
                            dim: dim as u32,
                        }),
                        stable_routing_keys: Vec::new(),
                    },
                ))
                .map_err(|error| format!("spill vector {id}: {error}"))?;
            let fields: Vec<AnalyzedField> = transposes
                .iter()
                .map(|transpose| {
                    transpose.field(row).ok_or_else(|| {
                        format!(
                            "{} segment {}: row {row} is past the transpose",
                            set.root().display(),
                            meta.segment_id
                        )
                    })
                })
                .collect::<Result<_, _>>()?;
            let slot = &mut sidecars[bucket as usize];
            if slot.is_none() {
                let path = analysis_sidecar_path(spills[bucket as usize].dir(), bucket);
                let file = std::fs::File::create(&path)
                    .map_err(|error| format!("create {}: {error}", path.display()))?;
                *slot = Some(std::io::BufWriter::with_capacity(1 << 20, file));
            }
            write_analysis_entry(slot.as_mut().expect("opened above"), id, &fields)
                .map_err(|error| format!("spill analysis of {id}: {error}"))?;
        }
        // This segment is read; its pages leave the resident set and the
        // page cache, so the run's footprint is one segment's.
        drop(transposes);
        if let Some(store) = exact.as_ref() {
            store.release_pages().map_err(|e| {
                format!("{} segment {}: {e}", set.root().display(), meta.segment_id)
            })?;
        }
        bm25.release_pages()
            .map_err(|e| format!("{} segment {}: {e}", set.root().display(), meta.segment_id))?;
    }
    for writer in sidecars.iter_mut().flatten() {
        use std::io::Write;
        writer
            .flush()
            .map_err(|error| format!("flush analysis sidecar: {error}"))?;
    }
    drop(sidecars);
    let spill_dirs: Vec<PathBuf> = spills
        .iter_mut()
        .map(|writer| {
            writer
                .flush()
                .map(|()| writer.dir().to_path_buf())
                .map_err(|error| format!("flush spill log {}: {error}", writer.dir().display()))
        })
        .collect::<Result<_, _>>()?;
    drop(spills);

    // The build-memory budget against the spill's own per-cut row counts:
    // the largest cut must fit alone (one thread still replays one cut),
    // and the thread count times it must fit together. A plan that
    // cannot fit is refused by name; nothing is lowered to fit.
    if let Some(budget) = options.build_memory {
        let threads = options.build_threads.max(1) as u64;
        if let Some((bucket, &rows)) = bucket_rows
            .iter()
            .enumerate()
            .filter(|(_, &n)| n > 0)
            .max_by_key(|(_, &n)| n)
        {
            let estimate = rows
                .checked_mul(BUILD_BYTES_PER_ROW)
                .ok_or_else(|| "build memory arithmetic overflow estimating a cut".to_string())?;
            if estimate > budget {
                return Err(format!(
                    "cut {bucket}: {rows} documents at {} KiB a row (the conservative end of \
                     the 40-70 KB band) is about {} MiB, over the build-memory budget of {} \
                     MiB for a single cut; raise the budget or cut finer",
                    BUILD_BYTES_PER_ROW / 1024,
                    estimate.div_ceil(1 << 20),
                    budget / (1 << 20)
                ));
            }
            let need = estimate.checked_mul(threads).ok_or_else(|| {
                "build memory arithmetic overflow multiplying the cut estimate by build threads"
                    .to_string()
            })?;
            if need > budget {
                return Err(format!(
                    "cut {bucket}: {rows} documents at {} KiB a row is about {} MiB, and \
                     {threads} build threads need {} MiB at once, over the build-memory budget \
                     of {} MiB; lower the thread count, raise the budget, or cut finer -- the \
                     thread count is not lowered for you",
                    BUILD_BYTES_PER_ROW / 1024,
                    estimate.div_ceil(1 << 20),
                    need.div_ceil(1 << 20),
                    budget / (1 << 20)
                ));
            }
        }
    }

    // Pass 3: one image per cut, in cut order. One cut replayed and
    // sealed at `slot`; `None` for an empty cut.
    let hnsw_sources = HnswCompactionSources::from_segment_set(slot_offset, set);
    let build = |bucket: u32,
                 ordinal: usize,
                 slot: u64,
                 analyze: &mut Analyzer|
     -> Result<Option<(ChildImage, u64)>, String> {
        let mut replay = Replay::default();
        let spill_dir = &spill_dirs[bucket as usize];
        replay_buckets_routed(spill_dir, 0..1, 1, dim, false, u64::MAX, false, &mut replay)?;
        replay.analyzed = read_analysis_file(&analysis_sidecar_path(spill_dir, bucket))?;
        replay.compact();
        let rows = live_rows(&replay);
        if rows == 0 {
            return Ok(None);
        }
        // Key order within the cut, ties by source id; vector-bearing
        // rows first, the slot shape the image builder assigns.
        let mut keyed: Vec<(i64, u64)> = Vec::with_capacity(replay.documents.len());
        let mut order: Vec<u64> = Vec::with_capacity(replay.documents.len());
        for id in replay.documents.keys() {
            match partition_key_of(&replay.documents[id], column)? {
                Some(key) => keyed.push((key, *id)),
                None => order.push(*id),
            }
        }
        keyed.sort_unstable();
        let mut ordered: Vec<u64> = keyed.into_iter().map(|(_, id)| id).collect();
        ordered.extend(order);
        let mut final_order: Vec<u64> = ordered
            .iter()
            .copied()
            .filter(|id| replay.vectors.contains_key(id))
            .collect();
        final_order.extend(
            ordered
                .iter()
                .copied()
                .filter(|id| !replay.vectors.contains_key(id)),
        );
        let image = finish_child_ordered_with_sources(
            &manifest,
            replay,
            &final_order,
            ordinal,
            out_dir,
            slot,
            0,
            u64::MAX,
            Some(&tables.fields),
            Some(&tables.fingerprints),
            Some(&binding),
            derived.as_ref(),
            Some(&tables.columns),
            hnsw_sources.as_ref(),
            analyze,
        )?;
        Ok(Some((image, rows)))
    };
    let mut images: Vec<ChildImage> = Vec::new();
    let mut id_map: BTreeMap<u64, u64> = BTreeMap::new();
    let mut place = |ordinal: usize, image: ChildImage| -> Result<(), String> {
        for (slot, &old_id) in image.row_parent_ids.iter().enumerate() {
            let new_local = image
                .slot_offset
                .checked_add(slot as u64)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
            if id_map.insert(old_id, new_local).is_some() {
                return Err(format!(
                    "compaction from segments saw source id {old_id} twice; the catalog is corrupt"
                ));
            }
        }
        debug_assert_eq!(ordinal, images.len());
        images.push(image);
        Ok(())
    };
    if options.build_threads <= 1 {
        let mut next_slot = 0u64;
        let mut ordinal = 0usize;
        let mut refuse = |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| -> Result<Vec<AnalyzedDoc>, String> {
            Err("a compaction from segments analyzes nothing; every row's fields come from the \
                 segments"
                .to_string())
        };
        for bucket in 0..plan.buckets {
            let Some((image, rows)) = build(bucket, ordinal, next_slot, &mut refuse)? else {
                continue;
            };
            place(ordinal, image)?;
            next_slot = next_slot
                .checked_add(rows)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
            ordinal += 1;
        }
    } else {
        // Threaded: every cut's slot range comes from the spill's own
        // counts, so cuts build in any order and append in cut order,
        // the same catalog the one-thread build writes.
        let mut plan_items: Vec<(u32, usize, u64)> = Vec::new();
        let mut slot = 0u64;
        for bucket in 0..plan.buckets {
            let n = bucket_rows[bucket as usize];
            if n == 0 {
                continue;
            }
            plan_items.push((bucket, plan_items.len(), slot));
            slot = slot
                .checked_add(n)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        let stop = std::sync::atomic::AtomicBool::new(false);
        let done: std::sync::Mutex<BuiltBuckets> = std::sync::Mutex::new(BTreeMap::new());
        let ready = std::sync::Condvar::new();
        let threads = options.build_threads.min(plan_items.len().max(1));
        // The backlog of sealed cuts waiting on the ordered append is
        // bounded: a worker waits for space BEFORE claiming a cut, never
        // while holding a finished one (the appender takes them in plan
        // order, so a worker holding the wanted cut while it waits for
        // space would deadlock). Workers may all pass the check at once,
        // so the backlog can reach the queue plus the thread count.
        let queue = options
            .build_queue
            .unwrap_or(options.build_threads)
            .min(plan_items.len().max(1));
        let outcome: Result<(), String> = std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    let mut refuse =
                        |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]|
                         -> Result<Vec<AnalyzedDoc>, String> {
                            Err("a compaction from segments analyzes nothing; every row's \
                                 fields come from the segments"
                                .to_string())
                        };
                    loop {
                        if stop.load(std::sync::atomic::Ordering::SeqCst) {
                            break;
                        }
                        {
                            let mut guard = done.lock().expect("cut results");
                            while guard.len() >= queue
                                && !stop.load(std::sync::atomic::Ordering::SeqCst)
                            {
                                guard = ready
                                    .wait_timeout(guard, std::time::Duration::from_millis(200))
                                    .expect("cut results")
                                    .0;
                            }
                        }
                        if stop.load(std::sync::atomic::Ordering::SeqCst) {
                            break;
                        }
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if i >= plan_items.len() {
                            break;
                        }
                        let (bucket, ordinal, slot) = plan_items[i];
                        let result = build(bucket, ordinal, slot, &mut refuse).and_then(|built| {
                            built.ok_or_else(|| {
                                format!(
                                    "cut {bucket}: the spill counted {} documents and the \
                                         replay found none",
                                    bucket_rows[bucket as usize]
                                )
                            })
                        });
                        let failed = result.is_err();
                        done.lock().expect("cut results").insert(i, result);
                        ready.notify_all();
                        if failed {
                            stop.store(true, std::sync::atomic::Ordering::SeqCst);
                            break;
                        }
                    }
                });
            }
            for (i, &(bucket, ordinal, _slot)) in plan_items.iter().enumerate() {
                let result = {
                    let mut guard = done.lock().expect("cut results");
                    loop {
                        if let Some(result) = guard.remove(&i) {
                            break result;
                        }
                        if stop.load(std::sync::atomic::Ordering::SeqCst) {
                            let first = guard
                                .values()
                                .find_map(|r| r.as_ref().err().cloned())
                                .unwrap_or_else(|| {
                                    "a cut build stopped without a result".to_string()
                                });
                            break Err(first);
                        }
                        guard = ready
                            .wait_timeout(guard, std::time::Duration::from_millis(200))
                            .expect("cut results")
                            .0;
                    }
                };
                ready.notify_all();
                let placed = result.and_then(|(image, rows)| {
                    if rows != bucket_rows[bucket as usize] {
                        return Err(format!(
                            "cut {bucket}: the spill counted {} documents, the sealed image \
                             holds {rows} rows",
                            bucket_rows[bucket as usize]
                        ));
                    }
                    place(ordinal, image)
                });
                if let Err(e) = placed {
                    stop.store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(e);
                }
            }
            Ok(())
        });
        outcome?;
    }
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(SegmentCompactionBuild {
        images,
        id_map,
        rows_before,
        tombstones,
        binding: Some(binding),
    })
}
