use super::compaction_build::{child_order, partition_key_of, plan_cuts, ChildCutPlan};
use super::placement_split::{finish_child_pinned, manifest_derived};
use super::*;
#[allow(clippy::too_many_arguments)]
pub fn split_placement_tree_logs(
    gens: &[PathBuf],
    tree: &crate::placement::PlacementTreeConfig,
    out_dir: &Path,
    slot_offsets: &[u64],
    bm25_fields: Option<&[String]>,
    options: TreeSplitOptions,
    analyze: &mut Analyzer,
) -> Result<TreeReshardOutput, String> {
    let placement = crate::placement::Placement::validate(tree)?;
    let children = tree_children(&placement)?;
    if let Some(only) = options.only_child {
        if matches!(options.layout, TreeChildLayout::SingleImage { .. }) {
            return Err(
                "--only-child builds one segmented child; a single-image split writes every child"
                    .to_string(),
            );
        }
        if only >= children.len() {
            return Err(format!(
                "--only-child={only} names no child; the tree has {} leaf shards (0..{})",
                children.len(),
                children.len().saturating_sub(1)
            ));
        }
    }
    if options.build_threads > 1 {
        if options.source != TreeRowSource::Segments {
            return Err(format!(
                "--build-threads={} builds buckets at once from transplanted fields; the log \
                 replay analyzes through one sidecar session, split with --from-segments or \
                 one thread",
                options.build_threads
            ));
        }
        if matches!(options.layout, TreeChildLayout::SingleImage { .. }) {
            return Err(format!(
                "--build-threads={} builds a segmented child's buckets at once; a single-image \
                 child is one image",
                options.build_threads
            ));
        }
    }
    if options.build_queue == Some(0) {
        return Err("--build-queue=0 holds no finished bucket; give it one or more".to_string());
    }
    if options.build_memory.is_some()
        && matches!(options.layout, TreeChildLayout::SingleImage { .. })
    {
        return Err(
            "--build-memory sizes the segmented build's bucket replays; a single-image child \
             is one replay, bounded by --single-image=<rows>"
                .to_string(),
        );
    }
    if slot_offsets.len() != children.len() {
        return Err(format!(
            "a re-placement split needs one slot offset per child: {} offsets for {} children \
             ({} leaves, shards summed)",
            slot_offsets.len(),
            children.len(),
            placement.leaves().len()
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("a re-placement split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
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
        top_generation = top_generation.max(current.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let column = placement.column().to_string();
    let first_child_of_leaf = |code: i64| children.iter().position(|child| child.code == code);
    let shards_of_leaf = |code: i64| children.iter().filter(|child| child.code == code).count();
    let spill_root = out_dir.join("spill");
    if matches!(options.cut, SpillCut::Column { .. })
        && matches!(options.layout, TreeChildLayout::SingleImage { .. })
    {
        return Err(
            "a column cut orders a child's segments; it needs the segmented layout".to_string(),
        );
    }
    let sources = match options.source {
        TreeRowSource::Logs => None,
        TreeRowSource::Segments => Some(open_segment_sources(gens)?),
    };
    if options.source == TreeRowSource::Segments {
        if let Some(child) = children.iter().find(|child| shards_of_leaf(child.code) > 1) {
            return Err(format!(
                "leaf {:?} has {} shards, which are tiled by stable routing keys, and sealed \
                 segments carry no routing keys; give the leaf one shard or split from the logs",
                child.leaf,
                shards_of_leaf(child.code)
            ));
        }
    }
    let source_columns: Option<ColumnTables> = match &sources {
        Some((_, tables)) => Some(tables.columns.clone()),
        None => manifest.columns.as_ref().map(|table| ColumnTables {
            facets: table.facets.clone(),
            numerics: table.numerics.clone(),
            map_facets: table.map_facets.clone(),
            map_numerics: table.map_numerics.clone(),
            map_integers: table.map_integers.clone(),
            map_unsigned_integers: table.map_unsigned_integers.clone(),
            integers: table.integers.clone(),
            unsigned_integers: table.unsigned_integers.clone(),
            geo: table.geo.clone(),
        }),
    };
    let declaration: Option<std::sync::Arc<crate::derived::Declaration>> = match &options.derived {
        Some(declaration) => Some(std::sync::Arc::clone(declaration)),
        None => manifest_derived(&manifest)?
            .map(|stored| stored.declaration().map(std::sync::Arc::new))
            .transpose()?,
    };
    if let Some(declaration) = declaration.as_deref() {
        let Some(columns) = source_columns.as_ref() else {
            return Err(
                "a split under a derived-column declaration needs the sources' column tables, \
                 which sealed segments carry and a log manifest written by a node that records \
                 them does; this log's manifest has none, so split from the segments"
                    .to_string(),
            );
        };
        let without = |names: &[String]| -> Vec<String> {
            names
                .iter()
                .filter(|name| !declaration.is_derived(name))
                .cloned()
                .collect()
        };
        let sources_tables = crate::values::NumericTypes {
            numerics: &without(&columns.numerics),
            integers: &without(&columns.integers),
            unsigned_integers: &without(&columns.unsigned_integers),
            map_numerics: &columns.map_numerics,
            map_integers: &columns.map_integers,
            map_unsigned_integers: &columns.map_unsigned_integers,
            facets: &columns.facets,
        };
        declaration
            .check_tables(&sources_tables)
            .map_err(|e| format!("derived columns: {e}"))?;
        for name in &options.derive {
            if !declaration.is_derived(name) {
                return Err(format!(
                    "--derive={name} names no column of the declaration ({:?})",
                    declaration
                        .columns()
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>()
                ));
            }
            if options.source == TreeRowSource::Segments
                && declaration
                    .column(name)
                    .is_some_and(|column| column.reads_stable_key)
            {
                return Err(format!(
                    "--derive={name} reads the stable routing key, and sealed segments carry \
                     none; split from the logs"
                ));
            }
        }
        if let SpillCut::Column { column, .. } = &options.cut {
            if options.derive.contains(column) {
                return Err(format!(
                    "the cut column {column:?} is being derived in this run; derive it first \
                     and cut on it in a second split"
                ));
            }
        }
    } else if !options.derive.is_empty() {
        return Err(format!(
            "--derive={:?} needs a declaration (--derived-columns)",
            options.derive
        ));
    }
    let rederive = |id: u64,
                    document: AddDocumentsRequest,
                    stable_key: Option<&[u8]>|
     -> Result<AddDocumentsRequest, String> {
        match declaration.as_deref() {
            Some(declaration) => declaration
                .rederive(document, &options.derive, stable_key)
                .map_err(|e| format!("live source id {id}: {}", e.message())),
            None if !document.derived_fingerprint.is_empty() => Err(format!(
                "live source id {id} was derived under declaration {:?}, and the children \
                 would be written under none; pass --derived-columns",
                document.derived_fingerprint
            )),
            None => Ok(document),
        }
    };
    let route = |id: u64,
                 document: &AddDocumentsRequest,
                 stable_key: Option<&Vec<u8>>|
     -> Result<(usize, i64, bool), String> {
        let carried = partition_key_of(document, &column)?;
        let leaf = placement
            .evaluate(document)
            .map_err(|e| format!("live source id {id}: {e}"))?;
        let moved = carried != Some(leaf.code);
        let base = first_child_of_leaf(leaf.code).expect("children cover every leaf");
        let n = shards_of_leaf(leaf.code);
        let index = if n == 1 {
            base
        } else {
            let Some(key) = stable_key.filter(|key| !key.is_empty()) else {
                return Err(format!(
                    "live source id {id} routes to leaf {:?}, which has {n} shards, and \
                     carries no stable key to pick one by; give the leaf one shard or \
                     rebuild the rows through routed ingest",
                    leaf.name
                ));
            };
            let hash = crate::coordinator::stable_routing_hash(key);
            base + children[base..base + n]
                .iter()
                .position(|child| hash >= child.hash_lo && hash <= child.hash_hi)
                .expect("the tiles cover the hash space")
        };
        Ok((index, leaf.code, moved))
    };
    let mut cut_plans: Option<Vec<ChildCutPlan>> = match &options.cut {
        SpillCut::Hash => None,
        SpillCut::Column {
            column: cut_column,
            rows_per_cut,
        } => {
            if *rows_per_cut == 0 {
                return Err("a column cut needs a positive row bound".to_string());
            }
            if cut_column == &column {
                return Err(format!(
                    "the cut column {cut_column:?} is the placement column; every row of a \
                     child carries one code there, cut by another column"
                ));
            }
            let mut histograms: Vec<BTreeMap<i64, u64>> =
                (0..children.len()).map(|_| BTreeMap::new()).collect();
            let mut unkeyed = vec![0u64; children.len()];
            match &sources {
                Some((shards, tables)) => {
                    if !tables
                        .columns
                        .integers
                        .iter()
                        .any(|name| name == cut_column)
                    {
                        return Err(format!(
                            "the cut column {cut_column:?} is not an integer column of the \
                             sources ({:?})",
                            tables.columns.integers
                        ));
                    }
                    for shard in shards {
                        for i in 0..shard.set.len() {
                            let bm25 = shard.set.bm25(i);
                            let rows = bm25.next_doc_id();
                            for row in 0..rows {
                                let label = shard.set.metadata(i).base_label + u64::from(row);
                                if !shard.is_live(i, row, label) {
                                    continue;
                                }
                                let id = shard.slot_offset + label;
                                let document = reconstruct_document(bm25, row, tables, false)?;
                                let document = rederive(id, document, None)?;
                                let (index, _, _) = route(id, &document, None)?;
                                match partition_key_of(&document, cut_column)? {
                                    Some(key) => *histograms[index].entry(key).or_insert(0) += 1,
                                    None => unkeyed[index] += 1,
                                }
                            }
                        }
                    }
                }
                None => {
                    for gen in gens {
                        let current = read_gen_manifest(gen)?;
                        for bucket in 0..current.bucket_count {
                            let mut replay = Replay::default();
                            replay_buckets_through(
                                gen,
                                bucket..bucket + 1,
                                current.bucket_count as usize,
                                current.dim as usize,
                                false,
                                u64::MAX,
                                &mut replay,
                            )?;
                            replay.compact();
                            for (id, document) in &replay.documents {
                                let key = replay.stable_keys.get(id);
                                let document =
                                    rederive(*id, document.clone(), key.map(Vec::as_slice))?;
                                let (index, _, _) = route(*id, &document, key)?;
                                match partition_key_of(&document, cut_column)? {
                                    Some(key) => *histograms[index].entry(key).or_insert(0) += 1,
                                    None => unkeyed[index] += 1,
                                }
                            }
                        }
                    }
                }
            }
            Some(plan_cuts(&histograms, &unkeyed, *rows_per_cut))
        }
    };
    let spill_bucket_bits = match (&cut_plans, options.spill_bucket_bits) {
        (Some(plans), _) => {
            let widest = plans
                .iter()
                .map(|plan| plan.buckets)
                .max()
                .unwrap_or(1)
                .max(1);
            let bits = widest.next_power_of_two().trailing_zeros();
            if bits > 16 {
                return Err(format!(
                    "the column cut needs {widest} buckets in one child, above 65,536; raise the \
                     row bound"
                ));
            }
            bits
        }
        (None, Some(bits)) => {
            if bits > 16 {
                return Err(format!(
                    "spill bucket bits {bits} exceed 16 (65,536 bucket files per child)"
                ));
            }
            bits
        }
        (None, None) => {
            let mut widest = manifest.bucket_count;
            for gen in gens {
                widest = widest.max(read_gen_manifest(gen)?.bucket_count);
            }
            widest.trailing_zeros()
        }
    };
    let mut spill_manifest = manifest.clone();
    spill_manifest.bucket_bits = spill_bucket_bits;
    spill_manifest.bucket_count = 1u32 << spill_bucket_bits;
    match declaration.as_deref() {
        Some(declaration) => {
            spill_manifest.derived_fingerprint = declaration.fingerprint().to_string();
            spill_manifest.derived = declaration.config();
        }
        None => {
            spill_manifest.derived_fingerprint.clear();
            spill_manifest.derived.clear();
        }
    }
    let spill_bucket_count = spill_manifest.bucket_count;
    {
        let spill_files = (0..children.len())
            .map(|index| match &cut_plans {
                Some(plans) => u64::from(plans[index].buckets),
                None => u64::from(spill_bucket_count),
            })
            .sum::<u64>()
            .saturating_mul(2);
        let source_files = sources.as_ref().map_or(0, |(shards, _)| {
            shards
                .iter()
                .map(|shard| shard.set.len() as u64)
                .sum::<u64>()
                .saturating_mul(2)
        });
        let needed = spill_files.saturating_add(source_files).saturating_add(64);
        let limit = match options.open_files_limit {
            Some(limit) => Some(limit),
            None => open_files_soft_limit(),
        };
        if let Some(limit) = limit {
            if needed > limit {
                return Err(format!(
                    "the split would hold about {needed} files open ({spill_files} spill logs and \
                     analysis sidecars over {} children, {source_files} for the sources' segments) \
                     above this process's open-files limit of {limit}; raise it (ulimit -n, or \
                     the reshard raises its own to the hard limit at start) or use fewer, larger \
                     cuts (--cut-rows)",
                    children.len()
                ));
            }
        }
    }
    let mut spills = Vec::with_capacity(children.len());
    for index in 0..children.len() {
        let dir = spill_root.join(format!("c{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    let mut sidecars: Vec<Vec<Option<std::io::BufWriter<std::fs::File>>>> = (0..children.len())
        .map(|_| (0..spill_bucket_count).map(|_| None).collect())
        .collect();
    let mut moved = 0u64;
    let mut placed = vec![0u64; children.len()];
    let mut bucket_docs: Vec<Vec<u64>> = (0..children.len())
        .map(|_| vec![0u64; spill_bucket_count as usize])
        .collect();
    let mut transplanted_rows = 0u64;
    let mut peak_transpose_bytes = 0u64;
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let cut_column: Option<&str> = match &options.cut {
        SpillCut::Column { column, .. } => Some(column.as_str()),
        SpillCut::Hash => None,
    };
    let rows_per_cut = match &options.cut {
        SpillCut::Column { rows_per_cut, .. } => *rows_per_cut,
        SpillCut::Hash => 0,
    };
    let mut file_row = |index: usize,
                        id: u64,
                        rewritten: AddDocumentsRequest,
                        vector: Option<(Vec<f32>, u32)>,
                        keys: Vec<Vec<u8>>,
                        fields: Option<&[AnalyzedField]>|
     -> Result<(), String> {
        let bucket = match (cut_plans.as_mut(), cut_column) {
            (Some(plans), Some(cut_column)) => {
                let key = partition_key_of(&rewritten, cut_column)?;
                Some(plans[index].bucket(key, rows_per_cut))
            }
            _ => None,
        };
        let document = wal_record::Op::AddDocuments(crate::pb::wal::LoggedAddDocuments {
            source_references: Vec::new(),
            first_id: id,
            documents: vec![rewritten],
            stable_routing_keys: keys.clone(),
        });
        match bucket {
            Some(bucket) => spills[index].append_to_bucket(bucket, document),
            None => spills[index].append(document),
        }
        .map_err(|error| format!("spill document {id}: {error}"))?;
        let filed = bucket.unwrap_or(bucket_of(id, spill_bucket_count as usize) as u32);
        bucket_docs[index][filed as usize] += 1;
        if let Some((vector, dim)) = vector {
            let record = wal_record::Op::AddVectors(crate::pb::wal::LoggedAddVectors {
                first_id: id,
                batch: Some(crate::pb::AddVectorsRequest {
                    vectors: vector,
                    dim,
                }),
                stable_routing_keys: keys,
            });
            match bucket {
                Some(bucket) => spills[index].append_to_bucket(bucket, record),
                None => spills[index].append(record),
            }
            .map_err(|error| format!("spill vector {id}: {error}"))?;
        }
        if let Some(fields) = fields {
            let bucket = bucket.unwrap_or(bucket_of(id, spill_bucket_count as usize) as u32);
            let slot = &mut sidecars[index][bucket as usize];
            if slot.is_none() {
                let path = analysis_sidecar_path(spills[index].dir(), bucket);
                let file = std::fs::File::create(&path)
                    .map_err(|error| format!("create {}: {error}", path.display()))?;
                *slot = Some(std::io::BufWriter::with_capacity(1 << 20, file));
            }
            write_analysis_entry(slot.as_mut().expect("opened above"), id, fields)
                .map_err(|error| format!("spill analysis of {id}: {error}"))?;
        }
        Ok(())
    };
    match &sources {
        None => {
            for gen in gens {
                let current = read_gen_manifest(gen)?;
                let bucket_count = current.bucket_count as usize;
                let dim = current.dim as usize;
                for bucket in 0..current.bucket_count {
                    let mut replay = Replay::default();
                    replay_buckets_through(
                        gen,
                        bucket..bucket + 1,
                        bucket_count,
                        dim,
                        false,
                        u64::MAX,
                        &mut replay,
                    )?;
                    replay.compact();
                    for id in child_order(&replay) {
                        if !seen.insert(id) {
                            return Err(format!(
                                "live source id {id} appears in more than one input generation; \
                                 the inputs overlap"
                            ));
                        }
                        let Some(document) = replay.documents.get(&id) else {
                            return Err(format!(
                                "live source id {id} has a vector and no document; a re-placement \
                                 split evaluates the tree on documents (a code-keyed split, \
                                 --placement-ranges, routes such a row to its default child)"
                            ));
                        };
                        let key = replay.stable_keys.get(&id);
                        let document = rederive(id, document.clone(), key.map(Vec::as_slice))?;
                        let (index, code, changed) = route(id, &document, key)?;
                        if changed {
                            moved += 1;
                        }
                        let mut rewritten = document;
                        rewritten.integers.retain(|value| value.field != column);
                        rewritten.integers.push(crate::pb::IntegerValue {
                            field: column.clone(),
                            value: code,
                        });
                        let keys: Vec<Vec<u8>> =
                            replay.stable_keys.get(&id).cloned().into_iter().collect();
                        let vector = replay
                            .vectors
                            .get(&id)
                            .map(|vector| (vector.clone(), current.dim));
                        file_row(index, id, rewritten, vector, keys, None)?;
                        placed[index] += 1;
                    }
                }
            }
        }
        Some((shards, tables)) => {
            for shard in shards {
                let current = read_gen_manifest(&shard.gen)?;
                for i in 0..shard.set.len() {
                    let meta = shard.set.metadata(i);
                    let bm25 = shard.set.bm25(i);
                    let exact = open_segment_exact(shard, i)?;
                    let rows = bm25.next_doc_id();
                    if u64::from(rows) != meta.rows {
                        return Err(format!(
                            "{} segment {}: the store has {rows} rows, the metadata {}",
                            shard.root.display(),
                            meta.segment_id,
                            meta.rows
                        ));
                    }
                    let mut transposes = Vec::with_capacity(bm25.field_count());
                    let mut bytes = 0u64;
                    for f in 0..bm25.field_count() {
                        let transpose = bm25.field(f).transpose().map_err(|e| {
                            format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                        })?;
                        bytes += transpose.bytes() as u64;
                        transposes.push(transpose);
                    }
                    peak_transpose_bytes = peak_transpose_bytes.max(bytes);
                    for row in 0..rows {
                        let label = meta.base_label + u64::from(row);
                        if !shard.is_live(i, row, label) {
                            continue;
                        }
                        let id = shard.slot_offset + label;
                        if !seen.insert(id) {
                            return Err(format!(
                                "live source id {id} appears in more than one source; the inputs \
                                 overlap"
                            ));
                        }
                        let document =
                            reconstruct_document(bm25, row, tables, true).map_err(|e| {
                                format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                            })?;
                        let document = rederive(id, document, None)?;
                        let (index, code, changed) = route(id, &document, None)?;
                        if changed {
                            moved += 1;
                        }
                        let mut rewritten = document;
                        rewritten.integers.retain(|value| value.field != column);
                        rewritten.integers.push(crate::pb::IntegerValue {
                            field: column.clone(),
                            value: code,
                        });
                        let vector = exact
                            .as_ref()
                            .map(|store| {
                                store
                                    .row_values(row as usize, row as usize + 1)
                                    .map(|values| (values, current.dim))
                            })
                            .transpose()
                            .map_err(|e| format!("read exact row {row}: {e}"))?;
                        let fields: Vec<AnalyzedField> = transposes
                            .iter()
                            .map(|transpose| {
                                transpose.field(row).ok_or_else(|| {
                                    format!(
                                        "{} segment {}: row {row} is past the transpose",
                                        shard.root.display(),
                                        meta.segment_id
                                    )
                                })
                            })
                            .collect::<Result<_, _>>()?;
                        file_row(index, id, rewritten, vector, Vec::new(), Some(&fields))?;
                        transplanted_rows += 1;
                        placed[index] += 1;
                    }
                    drop(transposes);
                    if let Some(store) = exact.as_ref() {
                        store.release_pages().map_err(|e| {
                            format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                        })?;
                    }
                    drop(exact);
                    bm25.release_pages().map_err(|e| {
                        format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                    })?;
                    drop_segment_cache(&shard.root, &meta.segment_id);
                }
            }
        }
    }
    drop(seen);
    for child in &mut sidecars {
        for writer in child.iter_mut().flatten() {
            use std::io::Write;
            writer
                .flush()
                .map_err(|error| format!("flush analysis sidecar: {error}"))?;
        }
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
    let (pinned_fingerprints, mut pinned_columns, pinned_fields): (
        Option<Vec<u64>>,
        Option<ColumnTables>,
        Option<Vec<String>>,
    ) = match &sources {
        Some((_, tables)) => (
            Some(tables.fingerprints.clone()),
            Some(tables.columns.clone()),
            Some(tables.fields.clone()),
        ),
        None => (None, source_columns.clone(), None),
    };
    if let (Some(declaration), Some(columns)) = (declaration.as_deref(), pinned_columns.as_mut()) {
        for (kind, table) in [
            (crate::pb::MaterializeKind::F64, &mut columns.numerics),
            (crate::pb::MaterializeKind::I64, &mut columns.integers),
            (
                crate::pb::MaterializeKind::U64,
                &mut columns.unsigned_integers,
            ),
        ] {
            for name in declaration.names_of(kind) {
                if !table.contains(&name) {
                    table.push(name);
                }
            }
        }
    }
    let derived = declaration
        .as_deref()
        .map(crate::postings::StoredDerived::of);
    let bm25_fields: Option<&[String]> = match &pinned_fields {
        Some(fields) => Some(fields.as_slice()),
        None => bm25_fields,
    };
    let check_routing = cut_plans.is_none();
    drop(cut_plans);
    if let Some(budget) = options.build_memory {
        let threads = options.build_threads.max(1) as u64;
        for (index, child) in children.iter().enumerate() {
            if options.only_child.is_some_and(|only| only != index) {
                continue;
            }
            let Some((bucket, &rows)) = bucket_docs[index]
                .iter()
                .enumerate()
                .filter(|(_, &n)| n > 0)
                .max_by_key(|(_, &n)| n)
            else {
                continue;
            };
            let estimate = rows.checked_mul(BUILD_BYTES_PER_ROW).ok_or_else(|| {
                "--build-memory arithmetic overflow estimating a bucket".to_string()
            })?;
            if estimate > budget {
                return Err(format!(
                    "child {index} ({}) bucket {bucket}: {rows} documents at {} KiB a row (the \
                     conservative end of the 40-70 KB band) is about {} MiB, over \
                     --build-memory={} MiB for a single bucket; raise the budget or cut finer \
                     (--cut-rows)",
                    child.leaf,
                    BUILD_BYTES_PER_ROW / 1024,
                    estimate.div_ceil(1 << 20),
                    budget / (1 << 20)
                ));
            }
            let need = estimate.checked_mul(threads).ok_or_else(|| {
                "--build-memory arithmetic overflow multiplying the bucket estimate by build threads".to_string()
            })?;
            if need > budget {
                return Err(format!(
                    "child {index} ({}) bucket {bucket}: {rows} documents at {} KiB a row is \
                     about {} MiB, and {threads} build threads need {} MiB at once, over \
                     --build-memory={} MiB; lower --build-threads, raise the budget, or cut \
                     finer (--cut-rows) -- the thread count is not lowered for you",
                    child.leaf,
                    BUILD_BYTES_PER_ROW / 1024,
                    estimate.div_ceil(1 << 20),
                    need.div_ceil(1 << 20),
                    budget / (1 << 20)
                ));
            }
        }
    }
    let dim = manifest.dim as usize;
    let generation = top_generation
        .checked_add(1)
        .ok_or_else(|| "re-placement split generation overflow".to_string())?;
    let slot_bound = |index: usize| -> Option<u64> {
        slot_offsets
            .iter()
            .copied()
            .filter(|offset| *offset > slot_offsets[index])
            .min()
    };
    let mut images = Vec::with_capacity(children.len());
    let mut segments = vec![0usize; children.len()];
    let mut peak_replay_rows = 0u64;
    match options.layout {
        TreeChildLayout::SingleImage { max_child_rows } => {
            for (index, child) in children.iter().enumerate() {
                if placed[index] > max_child_rows {
                    return Err(format!(
                        "child {index} ({}) has {} documents, above the single-image bound of \
                         {max_child_rows}; use the segmented layout or raise the bound",
                        child.leaf, placed[index]
                    ));
                }
            }
            for (index, (child, spill_dir)) in children.iter().zip(&spill_dirs).enumerate() {
                let mut replay = Replay::default();
                replay_buckets_routed(
                    spill_dir,
                    0..spill_bucket_count,
                    spill_bucket_count as usize,
                    dim,
                    false,
                    u64::MAX,
                    check_routing,
                    &mut replay,
                )?;
                if sources.is_some() {
                    for bucket in 0..spill_bucket_count {
                        replay
                            .analyzed
                            .extend(read_analysis_file(&analysis_sidecar_path(
                                spill_dir, bucket,
                            ))?);
                    }
                }
                replay.compact();
                if replay.documents.len() as u64 != placed[index] {
                    return Err(format!(
                        "child {index} ({}): the spill log holds {} documents, routing counted {}",
                        child.leaf,
                        replay.documents.len(),
                        placed[index]
                    ));
                }
                peak_replay_rows = peak_replay_rows.max(live_rows(&replay));
                let image = finish_child_pinned(
                    &manifest,
                    replay,
                    index,
                    out_dir,
                    slot_offsets[index],
                    child.hash_lo,
                    child.hash_hi,
                    bm25_fields,
                    pinned_fingerprints.as_deref(),
                    binding.as_ref(),
                    derived.as_ref(),
                    pinned_columns.as_ref(),
                    analyze,
                )?;
                check_slot_bound(
                    index,
                    &child.leaf,
                    image.slot_offset,
                    image_rows(&image),
                    slot_bound(index),
                )?;
                segments[index] = 1;
                images.push(image);
            }
        }
        TreeChildLayout::Segmented => {
            let backend_kind = manifest.backend_config()?.backend_kind;
            let work_root = out_dir.join("work");
            for (index, (child, spill_dir)) in children.iter().zip(&spill_dirs).enumerate() {
                let index_path = out_dir.join(format!("shard-{index}.tv"));
                let root = crate::node::segments_root(&index_path);
                if options.only_child.is_some_and(|only| only != index) {
                    eprintln!(
                        "reshard: child {index} ({}): not built (--only-child names another child)",
                        child.leaf
                    );
                    images.push(ChildImage {
                        vector_path: index_path,
                        exact_vector_path: root,
                        bm25_path: None,
                        slot_offset: slot_offsets[index],
                        hash_lo: child.hash_lo,
                        hash_hi: child.hash_hi,
                        num_vectors: 0,
                        num_documents: 0,
                        parent_ids: Vec::new(),
                        row_parent_ids: Vec::new(),
                    });
                    continue;
                }
                if root.exists() {
                    return Err(format!(
                        "child {index} ({}): {} already exists; a re-placement split writes a \
                         fresh catalog",
                        child.leaf,
                        root.display()
                    ));
                }
                let catalog = crate::segments::SegmentCatalog::open(&root)?;
                let work = work_root.join(format!("c{index:05}"));
                std::fs::create_dir_all(&work)
                    .map_err(|error| format!("mkdir {}: {error}", work.display()))?;
                let bound = slot_bound(index);
                let mut documents = 0u64;
                let mut vectors = 0u64;
                let build = |bucket: u32,
                             ordinal: usize,
                             slot: u64,
                             analyze: &mut Analyzer|
                 -> Result<Option<(ChildImage, u64)>, String> {
                    let mut replay = Replay::default();
                    replay_buckets_routed(
                        spill_dir,
                        bucket..bucket + 1,
                        spill_bucket_count as usize,
                        dim,
                        false,
                        u64::MAX,
                        check_routing,
                        &mut replay,
                    )?;
                    if sources.is_some() {
                        replay.analyzed =
                            read_analysis_file(&analysis_sidecar_path(spill_dir, bucket))?;
                    }
                    replay.compact();
                    let rows = live_rows(&replay);
                    if rows == 0 {
                        return Ok(None);
                    }
                    let image = finish_child_pinned(
                        &manifest,
                        replay,
                        ordinal,
                        &work,
                        slot,
                        child.hash_lo,
                        child.hash_hi,
                        bm25_fields,
                        pinned_fingerprints.as_deref(),
                        binding.as_ref(),
                        derived.as_ref(),
                        pinned_columns.as_ref(),
                        analyze,
                    )?;
                    Ok(Some((image, rows)))
                };
                let mut place = |bucket: u32,
                                 ordinal: usize,
                                 slot: u64,
                                 image: ChildImage|
                 -> Result<u64, String> {
                    let image_rows = image_rows(&image);
                    check_slot_bound(index, &child.leaf, slot, image_rows, bound)?;
                    if image.num_vectors != 0 && image.num_vectors != image.num_documents {
                        return Err(format!(
                            "child {index} ({}) bucket {bucket}: {} vectors but {} documents; a \
                             sealed segment is aligned (one document per vector) or has no \
                             vectors at all",
                            child.leaf, image.num_vectors, image.num_documents
                        ));
                    }
                    let Some(bm25_path) = image.bm25_path.as_deref() else {
                        return Err(format!(
                            "child {index} ({}) bucket {bucket}: no BM25 image was written for \
                             {} documents",
                            child.leaf, image.num_documents
                        ));
                    };
                    let live_path = crate::node::live_docs_sidecar_path(&image.vector_path);
                    crate::live_docs::LiveDocs::default()
                        .write(&live_path, image_rows)
                        .map_err(|error| {
                            format!("write live docs {}: {error}", live_path.display())
                        })?;
                    let segment_id = format!("g{generation}-c{index}-p{ordinal}");
                    let (vector_path, exact_path) = if image.num_vectors == 0 {
                        (None, None)
                    } else {
                        (
                            Some(image.vector_path.as_path()),
                            Some(image.exact_vector_path.as_path()),
                        )
                    };
                    catalog
                        .append(crate::segments::SegmentSource {
                            segment_id: &segment_id,
                            generation,
                            base_label: slot - slot_offsets[index],
                            backend_kind: &backend_kind,
                            vector_path,
                            exact_vector_path: exact_path,
                            bm25_path,
                            live_docs_path: &live_path,
                            partition_column: cut_column,
                        })
                        .map_err(|error| {
                            format!(
                                "child {index} ({}): seal segment {segment_id}: {error}",
                                child.leaf
                            )
                        })?;
                    for path in [
                        Some(image.vector_path.as_path()),
                        Some(image.exact_vector_path.as_path()),
                        Some(bm25_path),
                        Some(live_path.as_path()),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if path.exists() {
                            std::fs::remove_file(path).map_err(|error| {
                                format!("remove staged {}: {error}", path.display())
                            })?;
                        }
                    }
                    drop_page_cache(&wal::bucket_path(spill_dir, bucket));
                    drop_page_cache(&analysis_sidecar_path(spill_dir, bucket));
                    drop_segment_cache(&root, &segment_id);
                    documents += image.num_documents;
                    vectors += image.num_vectors;
                    Ok(image_rows)
                };
                let mut built = 0usize;
                if options.build_threads <= 1 {
                    let mut next_slot = slot_offsets[index];
                    for bucket in 0..spill_bucket_count {
                        let Some((image, rows)) = build(bucket, built, next_slot, analyze)? else {
                            continue;
                        };
                        peak_replay_rows = peak_replay_rows.max(rows);
                        let image_rows = place(bucket, built, next_slot, image)?;
                        next_slot = next_slot
                            .checked_add(image_rows)
                            .ok_or_else(|| "re-placement split slot overflow".to_string())?;
                        built += 1;
                    }
                } else {
                    let counts = &bucket_docs[index];
                    let mut plan: Vec<(u32, usize, u64)> = Vec::new();
                    let mut slot = slot_offsets[index];
                    for bucket in 0..spill_bucket_count {
                        let n = counts[bucket as usize];
                        if n == 0 {
                            continue;
                        }
                        plan.push((bucket, plan.len(), slot));
                        slot = slot
                            .checked_add(n)
                            .ok_or_else(|| "re-placement split slot overflow".to_string())?;
                    }
                    let next = std::sync::atomic::AtomicUsize::new(0);
                    let stop = std::sync::atomic::AtomicBool::new(false);
                    let done: std::sync::Mutex<BuiltBuckets> =
                        std::sync::Mutex::new(BTreeMap::new());
                    let ready = std::sync::Condvar::new();
                    let threads = options.build_threads.min(plan.len().max(1));
                    let queue = options
                        .build_queue
                        .unwrap_or(options.build_threads)
                        .min(plan.len().max(1));
                    let outcome: Result<(), String> = std::thread::scope(|scope| {
                        for _ in 0..threads {
                            scope.spawn(|| {
                                let mut refuse = |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]|
                                 -> Result<Vec<AnalyzedDoc>, String> {
                                    Err("a transplant from segments analyzes nothing; every \
                                         row's fields come from the spill"
                                        .to_string())
                                };
                                loop {
                                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                                        break;
                                    }
                                    {
                                        let mut guard = done.lock().expect("bucket results");
                                        while guard.len() >= queue
                                            && !stop.load(std::sync::atomic::Ordering::SeqCst)
                                        {
                                            guard = ready
                                                .wait_timeout(
                                                    guard,
                                                    std::time::Duration::from_millis(200),
                                                )
                                                .expect("bucket results")
                                                .0;
                                        }
                                    }
                                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                                        break;
                                    }
                                    let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                    if i >= plan.len() {
                                        break;
                                    }
                                    let (bucket, ordinal, slot) = plan[i];
                                    let result =
                                        build(bucket, ordinal, slot, &mut refuse).and_then(|built| {
                                            built.ok_or_else(|| {
                                                format!(
                                                    "child {index} ({}) bucket {bucket}: the \
                                                     spill counted {} documents and the replay \
                                                     found none",
                                                    child.leaf, counts[bucket as usize]
                                                )
                                            })
                                        });
                                    let failed = result.is_err();
                                    done.lock().expect("bucket results").insert(i, result);
                                    ready.notify_all();
                                    if failed {
                                        stop.store(true, std::sync::atomic::Ordering::SeqCst);
                                        break;
                                    }
                                }
                            });
                        }
                        for (i, &(bucket, ordinal, slot)) in plan.iter().enumerate() {
                            let result = {
                                let mut guard = done.lock().expect("bucket results");
                                loop {
                                    if let Some(result) = guard.remove(&i) {
                                        break result;
                                    }
                                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                                        let first = guard
                                            .values()
                                            .find_map(|r| r.as_ref().err().cloned())
                                            .unwrap_or_else(|| {
                                                "a bucket build stopped without a result"
                                                    .to_string()
                                            });
                                        break Err(first);
                                    }
                                    guard = ready
                                        .wait_timeout(guard, std::time::Duration::from_millis(200))
                                        .expect("bucket results")
                                        .0;
                                }
                            };
                            ready.notify_all();
                            let placed_rows = result.and_then(|(image, rows)| {
                                peak_replay_rows = peak_replay_rows.max(rows);
                                place(bucket, ordinal, slot, image)
                            });
                            let image_rows = match placed_rows {
                                Ok(rows) => rows,
                                Err(e) => {
                                    stop.store(true, std::sync::atomic::Ordering::SeqCst);
                                    return Err(e);
                                }
                            };
                            if image_rows != counts[bucket as usize] {
                                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                                return Err(format!(
                                    "child {index} ({}) bucket {bucket}: the spill counted {} \
                                     documents, the sealed segment holds {image_rows} rows",
                                    child.leaf, counts[bucket as usize]
                                ));
                            }
                        }
                        Ok(())
                    });
                    outcome?;
                    built = plan.len();
                }
                segments[index] += built;
                if documents != placed[index] {
                    return Err(format!(
                        "child {index} ({}): the sealed segments hold {documents} documents, \
                         routing counted {}",
                        child.leaf, placed[index]
                    ));
                }
                if let Some(column) = cut_column {
                    catalog
                        .publish_partition_key(Some(column.to_string()))
                        .map_err(|error| {
                            format!(
                                "child {index} ({}): publish partition key: {error}",
                                child.leaf
                            )
                        })?;
                }
                std::fs::remove_dir_all(&work)
                    .map_err(|error| format!("remove work {}: {error}", work.display()))?;
                eprintln!(
                    "reshard: child {index} ({}): {vectors} vectors, {documents} documents in {} \
                     segments -> {}",
                    child.leaf,
                    segments[index],
                    index_path.display()
                );
                images.push(ChildImage {
                    vector_path: index_path,
                    exact_vector_path: root,
                    bm25_path: None,
                    slot_offset: slot_offsets[index],
                    hash_lo: child.hash_lo,
                    hash_hi: child.hash_hi,
                    num_vectors: vectors,
                    num_documents: documents,
                    parent_ids: Vec::new(),
                    row_parent_ids: Vec::new(),
                });
            }
            if work_root.exists() {
                std::fs::remove_dir_all(&work_root)
                    .map_err(|error| format!("remove work {}: {error}", work_root.display()))?;
            }
        }
    }
    std::fs::remove_dir_all(&spill_root)
        .map_err(|error| format!("remove spill {}: {error}", spill_root.display()))?;
    Ok(TreeReshardOutput {
        derived: declaration
            .as_deref()
            .map(|declaration| declaration.config())
            .unwrap_or_default(),
        images: ReshardOutput {
            generation,
            children: images,
        },
        children,
        moved,
        placed,
        layout: options.layout,
        segments,
        peak_replay_rows,
        spill_bucket_count,
        source: options.source,
        cut: options.cut,
        peak_transpose_bytes,
        transplanted_rows,
    })
}
