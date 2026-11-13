use super::placement_split::{finish_child_ordered, finish_child_pinned};
use super::*;
pub struct CompactedRow<'a> {
    pub new_local: u64,
    pub old_id: u64,
    pub vector: Option<&'a [f32]>,
    pub document: Option<&'a AddDocumentsRequest>,
    pub stable_key: Option<&'a [u8]>,
}
pub type RowSink<'a> = dyn FnMut(CompactedRow<'_>) -> Result<(), String> + 'a;
#[derive(Debug)]
pub struct CompactionBuild {
    pub generation: u64,
    pub rows_before: u64,
    pub tombstones: u64,
    pub id_map: BTreeMap<u64, u64>,
    pub images: Vec<ChildImage>,
    pub binding: Option<crate::postings::StoredBinding>,
}
pub(super) fn emit_rows(
    replay: &Replay,
    slot_base: u64,
    id_map: &mut BTreeMap<u64, u64>,
    sink: &mut RowSink,
) -> Result<u64, String> {
    let order = child_order(replay);
    emit_rows_in(replay, &order, slot_base, id_map, sink)
}
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
            document: replay.documents.get(old_id),
            stable_key: replay.stable_keys.get(old_id).map(Vec::as_slice),
        })?;
    }
    Ok(order.len() as u64)
}
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
/// The generation must be full history with a usable provider
/// configuration, exactly as for a reshard; those checks refuse here
/// before anything is read. `columns` gives each image the shard's
/// column tables, so a bucket that holds no record of a declared column
/// still declares it and the outputs open under the node's
/// configuration.
#[allow(clippy::too_many_arguments)]
pub fn compact_log(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    segmented: bool,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let manifest = read_gen_manifest(gen)?;
    require_backend_config(&manifest, gen)?;
    require_complete_history(&manifest, gen)?;
    let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let bucket_count = manifest.bucket_count as usize;
    let dim = manifest.dim as usize;
    let generation = manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| "compaction generation overflow".to_string())?;
    let mut id_map = BTreeMap::new();
    let mut images = Vec::new();
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    if !segmented {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            0..manifest.bucket_count,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        let (present, tombstoned) = replay_counts(&replay);
        rows_before = present;
        tombstones = tombstoned;
        replay.compact();
        emit_rows(&replay, 0, &mut id_map, sink)?;
        images.push(finish_child_pinned(
            &manifest,
            replay,
            0,
            out_dir,
            0,
            0,
            u64::MAX,
            bm25_fields,
            pinned_fingerprints,
            binding.as_ref(),
            derived,
            columns,
            analyze,
        )?);
    } else {
        let mut next_slot = 0u64;
        for bucket in 0..manifest.bucket_count {
            let mut replay = Replay::default();
            replay_buckets_through(
                gen,
                bucket..bucket + 1,
                bucket_count,
                dim,
                false,
                cutoff_clock,
                &mut replay,
            )?;
            let (present, tombstoned) = replay_counts(&replay);
            rows_before += present;
            tombstones += tombstoned;
            replay.compact();
            let rows = emit_rows(&replay, next_slot, &mut id_map, sink)?;
            if rows == 0 {
                continue;
            }
            let ordinal = images.len();
            images.push(finish_child_pinned(
                &manifest,
                replay,
                ordinal,
                out_dir,
                next_slot,
                0,
                u64::MAX,
                bm25_fields,
                pinned_fingerprints,
                binding.as_ref(),
                derived,
                columns,
                analyze,
            )?);
            next_slot = next_slot
                .checked_add(rows)
                .ok_or_else(|| "compaction slot overflow".to_string())?;
        }
    }
    Ok(CompactionBuild {
        generation,
        rows_before,
        tombstones,
        id_map,
        images,
        binding,
    })
}
/// Where a child's rows go under a column cut: the cut of each value,
/// a run longer than the bound split by row count, unkeyed rows last.
#[derive(Debug, Clone, Default)]
pub(super) struct ChildCutPlan {
    bounds: Vec<i64>,
    long_runs: BTreeMap<i64, (u32, u32)>,
    filed: BTreeMap<i64, u64>,
    unkeyed: u32,
    pub(super) buckets: u32,
}
impl ChildCutPlan {
    fn plan(histogram: &BTreeMap<i64, u64>, unkeyed_rows: u64, rows_per_cut: u64) -> Self {
        let mut bounds = Vec::new();
        let mut long_runs = BTreeMap::new();
        let mut current = 0u64;
        let mut open = false;
        for (&key, &count) in histogram {
            if count > rows_per_cut {
                if open {
                    bounds.push(key - 1);
                    open = false;
                    current = 0;
                }
                let cuts = count.div_ceil(rows_per_cut) as u32;
                long_runs.insert(key, (bounds.len() as u32, cuts));
                for _ in 0..cuts {
                    bounds.push(key);
                }
                continue;
            }
            if open && current + count > rows_per_cut {
                bounds.push(key - 1);
                current = 0;
            }
            open = true;
            current += count;
        }
        if open {
            bounds.push(i64::MAX);
        }
        let keyed = bounds.len() as u32;
        let has_unkeyed = unkeyed_rows > 0;
        Self {
            bounds,
            long_runs,
            filed: BTreeMap::new(),
            unkeyed: keyed,
            buckets: keyed + u32::from(has_unkeyed),
        }
    }
    pub(super) fn bucket(&mut self, key: Option<i64>, rows_per_cut: u64) -> u32 {
        let Some(key) = key else {
            return self.unkeyed;
        };
        if let Some(&(first, cuts)) = self.long_runs.get(&key) {
            let filed = self.filed.entry(key).or_insert(0);
            let cut = (*filed / rows_per_cut) as u32;
            *filed += 1;
            return first + cut.min(cuts - 1);
        }
        self.bounds.partition_point(|bound| *bound < key) as u32
    }
}
/// Cut plans for every child from the (child, value) histograms.
pub(super) fn plan_cuts(
    histograms: &[BTreeMap<i64, u64>],
    unkeyed: &[u64],
    rows_per_cut: u64,
) -> Vec<ChildCutPlan> {
    histograms
        .iter()
        .zip(unkeyed)
        .map(|(histogram, &unkeyed)| ChildCutPlan::plan(histogram, unkeyed, rows_per_cut))
        .collect()
}
/// The partition key of one logged document: its value in `column`,
/// from the integer list or the timestamp list (both file into the same
/// column at ingest), or `None` when the document does not carry it.
pub(super) fn partition_key_of(
    document: &AddDocumentsRequest,
    column: &str,
) -> Result<Option<i64>, String> {
    if let Some(value) = document.integers.iter().find(|value| value.field == column) {
        return Ok(Some(value.value));
    }
    if let Some(value) = document
        .timestamps
        .iter()
        .find(|value| value.field == column)
    {
        let Some(instant) = value.value.as_ref() else {
            return Ok(None);
        };
        return crate::node::timestamp_to_epoch_micros(column, instant)
            .map(Some)
            .map_err(|status| status.message().to_string());
    }
    Ok(None)
}
/// One output of the cut: the live source ids it holds, in slot order
/// (key ascending, then source id; unkeyed rows in id order).
pub(super) struct Partition {
    pub(super) ids: Vec<u64>,
}
/// Cut keyed rows (sorted by key, then id) into partitions of at most
/// `bound` rows. A cut prefers a key boundary: a run of equal keys moves
/// to the next partition as a unit when it would overflow the current
/// one, so partitions cover disjoint key ranges wherever the data allows;
/// only a run longer than the bound is split, and then the two
/// partitions share that one key. Unkeyed rows follow in id order, cut
/// at the same bound.
pub(super) fn cut_partitions(
    keyed: &[(i64, u64)],
    unkeyed: &[u64],
    bound: usize,
) -> Vec<Partition> {
    let bound = bound.max(1);
    let mut partitions = Vec::new();
    let mut current: Vec<u64> = Vec::new();
    let mut i = 0;
    while i < keyed.len() {
        let key = keyed[i].0;
        let mut j = i;
        while j < keyed.len() && keyed[j].0 == key {
            j += 1;
        }
        let run = &keyed[i..j];
        if !current.is_empty() && current.len() + run.len() > bound {
            partitions.push(Partition {
                ids: std::mem::take(&mut current),
            });
        }
        for piece in run.chunks(bound) {
            if !current.is_empty() && current.len() + piece.len() > bound {
                partitions.push(Partition {
                    ids: std::mem::take(&mut current),
                });
            }
            current.extend(piece.iter().map(|(_, id)| *id));
        }
        i = j;
    }
    if !current.is_empty() {
        partitions.push(Partition { ids: current });
    }
    for piece in unkeyed.chunks(bound) {
        partitions.push(Partition {
            ids: piece.to_vec(),
        });
    }
    partitions
}
/// [`compact_log`] for the partitioned layout: the dense all-live rows
/// of ONE generation through `cutoff_clock`, ordered by `spec.column`
/// and cut into segments of at most `spec.bound` rows
/// (docs/immutable-segments.md "Partitioned layout"). Three passes, none
/// holding more than one WAL bucket or one output partition in memory:
///
/// 1. each bucket replays in turn and yields `(key, id)` per live row,
///    which the cut turns into partitions;
/// 2. each bucket replays again and its live rows are appended to one
///    spill log per partition, a single-bucket WAL generation under
///    `out_dir`;
/// 3. each spill log replays into its image, rows in key order, and
///    `sink` receives the rows in that slot order.
///
/// Rows whose document lacks the column, and vector-only rows, form the
/// unkeyed partitions after the keyed ones. A column no live row carries
/// is an error, as is a value that is not an integer. `columns` gives
/// every output the shard's column tables, so a partition that holds no
/// record of a column (the unkeyed one has no partition column at all)
/// still declares it.
#[allow(clippy::too_many_arguments)]
pub fn compact_log_partitioned(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let manifest = read_gen_manifest(gen)?;
    require_backend_config(&manifest, gen)?;
    require_complete_history(&manifest, gen)?;
    let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let bucket_count = manifest.bucket_count as usize;
    let dim = manifest.dim as usize;
    let generation = manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| "compaction generation overflow".to_string())?;
    let column = spec.column;
    let mut keyed: Vec<(i64, u64)> = Vec::new();
    let mut unkeyed: Vec<u64> = Vec::new();
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        let (present, tombstoned) = replay_counts(&replay);
        rows_before += present;
        tombstones += tombstoned;
        replay.compact();
        for id in child_order(&replay) {
            match replay.documents.get(&id) {
                Some(document) => match partition_key_of(document, column)? {
                    Some(key) => keyed.push((key, id)),
                    None => unkeyed.push(id),
                },
                None => unkeyed.push(id),
            }
        }
    }
    if keyed.is_empty() {
        return Err(format!(
            "partition column {column:?}: no live document of this shard carries it"
        ));
    }
    keyed.sort_unstable();
    unkeyed.sort_unstable();
    let partitions = cut_partitions(&keyed, &unkeyed, spec.bound);
    let mut partition_of: Vec<(u64, u32)> = partitions
        .iter()
        .enumerate()
        .flat_map(|(index, partition)| partition.ids.iter().map(move |id| (*id, index as u32)))
        .collect();
    partition_of.sort_unstable();
    drop(keyed);
    drop(unkeyed);
    let spill_root = out_dir.join("spill");
    let mut spill_manifest = manifest.clone();
    spill_manifest.bucket_bits = 0;
    spill_manifest.bucket_count = 1;
    let mut spills = Vec::with_capacity(partitions.len());
    for index in 0..partitions.len() {
        let dir = spill_root.join(format!("p{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        replay.compact();
        for id in child_order(&replay) {
            let index = partition_of
                .binary_search_by_key(&id, |(id, _)| *id)
                .map(|at| partition_of[at].1 as usize)
                .map_err(|_| format!("compaction lost track of source id {id} between passes"))?;
            let keys: Vec<Vec<u8>> = replay.stable_keys.get(&id).cloned().into_iter().collect();
            if let Some(document) = replay.documents.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddDocuments(
                        crate::pb::wal::LoggedAddDocuments {
                            source_references: Vec::new(),
                            first_id: id,
                            documents: vec![document.clone()],
                            stable_routing_keys: keys.clone(),
                        },
                    ))
                    .map_err(|error| format!("spill document {id}: {error}"))?;
            }
            if let Some(vector) = replay.vectors.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddVectors(
                        crate::pb::wal::LoggedAddVectors {
                            first_id: id,
                            batch: Some(crate::pb::AddVectorsRequest {
                                vectors: vector.clone(),
                                dim: manifest.dim,
                            }),
                            stable_routing_keys: keys,
                        },
                    ))
                    .map_err(|error| format!("spill vector {id}: {error}"))?;
            }
        }
    }
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
    let mut id_map = BTreeMap::new();
    let mut images = Vec::with_capacity(partitions.len());
    let mut next_slot = 0u64;
    for (index, (partition, spill_dir)) in partitions.iter().zip(&spill_dirs).enumerate() {
        let mut replay = Replay::default();
        replay_buckets_through(spill_dir, 0..1, 1, dim, false, u64::MAX, &mut replay)?;
        replay.compact();
        let held = replay.vectors.len().max(replay.documents.len());
        if held != partition.ids.len() {
            return Err(format!(
                "partition {index}: the spill log holds {held} rows, the cut assigned {}",
                partition.ids.len()
            ));
        }
        let mut order: Vec<u64> = partition
            .ids
            .iter()
            .copied()
            .filter(|id| replay.vectors.contains_key(id))
            .collect();
        order.extend(
            partition
                .ids
                .iter()
                .copied()
                .filter(|id| !replay.vectors.contains_key(id)),
        );
        let rows = emit_rows_in(&replay, &order, next_slot, &mut id_map, sink)?;
        images.push(finish_child_ordered(
            &manifest,
            replay,
            &order,
            index,
            out_dir,
            next_slot,
            0,
            u64::MAX,
            bm25_fields,
            pinned_fingerprints,
            binding.as_ref(),
            derived,
            columns,
            analyze,
        )?);
        next_slot = next_slot
            .checked_add(rows)
            .ok_or_else(|| "compaction slot overflow".to_string())?;
    }
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(CompactionBuild {
        generation,
        rows_before,
        tombstones,
        id_map,
        images,
        binding,
    })
}
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
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let mut manifest = WalManifest {
        dim: dim as u32,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        bit_width: 0,
        calibration_shift: Vec::new(),
        calibration_scale: Vec::new(),
        collection: String::new(),
        slot_offset,
        generation: 0,
        bucket_bits: 0,
        bucket_count: 1,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: wal::FORMAT_VERSION,
        derived_fingerprint: String::new(),
        legacy_derived_tag27: false,
        derived: Vec::new(),
        columns: None,
    };
    manifest.set_backend_config(backend_config);
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
        let image = finish_child_ordered(
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
