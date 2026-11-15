//! Write-ahead log manifest validation, opening, and append degradation.

use crate::node::*;

pub(in crate::node) fn wire_backend_config(
    config: &VectorBackendConfig,
) -> WireVectorBackendConfig {
    WireVectorBackendConfig {
        backend_kind: config.backend_kind.clone(),
        config_format: config.config_format.clone(),
        payload: config.payload.clone(),
    }
}

pub(in crate::node) fn internal_backend_config(
    config: WireVectorBackendConfig,
) -> Result<VectorBackendConfig, Status> {
    if config.backend_kind.trim().is_empty() {
        return Err(Status::invalid_argument("vector backend kind is required"));
    }
    if config.config_format.trim().is_empty() {
        return Err(Status::invalid_argument(
            "vector backend config format is required",
        ));
    }
    Ok(VectorBackendConfig {
        backend_kind: config.backend_kind,
        config_format: config.config_format,
        payload: config.payload,
    })
}

pub(in crate::node) fn wire_backend_descriptor(index: &VectorIndex) -> WireVectorBackendDescriptor {
    let descriptor = index.descriptor();
    WireVectorBackendDescriptor {
        backend_kind: descriptor.backend_kind,
        backend_version: descriptor.backend_version,
        dim: descriptor.dimension.unwrap_or(0) as u32,
        bits_per_dimension: descriptor.bits_per_dimension.unwrap_or(0),
        metric: descriptor.metric,
        score_direction: match descriptor.score_direction {
            ScoreDirection::HigherIsBetter => VectorScoreDirection::HigherIsBetter as i32,
            ScoreDirection::LowerIsBetter => VectorScoreDirection::LowerIsBetter as i32,
        },
        scoring_fingerprint: descriptor.scoring_fingerprint,
        quality_contract: match descriptor.quality_contract {
            QualityContract::ExhaustiveQuantized => {
                VectorQualityContract::ExhaustiveNativeScore as i32
            }
            QualityContract::ConfiguredAnn => VectorQualityContract::ConfiguredAnn as i32,
            QualityContract::ProbabilisticBound => VectorQualityContract::ProbabilisticBound as i32,
        },
        capabilities: descriptor
            .capabilities
            .into_iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
    }
}

/// Build the manifest describing the shard's current provider state and
/// dimension. An empty shard completes it lazily through backend configuration
/// or its first batch, before any records make the manifest immutable.
///
/// `preexisting` is the (vectors, documents) the shard already holds
/// that this generation's log will NOT contain — the installed image on
/// a snapshot rotation, or the whole shard when logging is enabled on an
/// already-populated index. Nonzero preexisting state marks the log as
/// partial history, which the reshard tool refuses (a log-only replay
/// would silently drop that state).
pub(crate) fn wal_manifest(
    index: Option<&VectorIndex>,
    config: &NodeConfig,
    generation: u64,
    preexisting: (u64, u64),
) -> wal::WalManifest {
    let backend_config = index.and_then(|index| index.backend_config().ok());
    wal::WalManifest {
        dim: index.map_or(0, |index| index.dim_opt().unwrap_or(0) as u32),
        vector_backend: backend_config
            .as_ref()
            .map_or_else(|| config.vector_backend.clone(), |c| c.backend_kind.clone()),
        vector_config_format: backend_config
            .as_ref()
            .map_or_else(String::new, |c| c.config_format.clone()),
        vector_config_payload: backend_config.map_or_else(Vec::new, |c| c.payload),
        collection: config.collection.clone(),
        slot_offset: config.slot_offset,
        generation,
        bucket_bits: config.wal_buckets.trailing_zeros(),
        bucket_count: config.wal_buckets,
        preexisting_vectors: preexisting.0,
        preexisting_documents: preexisting.1,
        format_version: wal::FORMAT_VERSION,
        derived_fingerprint: config
            .derived
            .as_ref()
            .map(|declaration| declaration.fingerprint().to_string())
            .unwrap_or_default(),
        derived: config
            .derived
            .as_ref()
            .map(|declaration| declaration.config())
            .unwrap_or_default(),
        columns: Some(wal::ColumnTable::of_config(config)),
    }
}

/// The persisted document tip of a shard: `next_doc_id` of the on-disk
/// BM25 sidecar (generation-aware), 0 when none exists. Opened read-only
/// and dropped — the serving copy is attached separately (`with_bm25`);
/// this exists so WAL reconciliation can know the applied tip without
/// depending on attachment order.
fn persisted_doc_tip(index_path: &Path) -> u64 {
    // Panic, like the rest of the WAL open path: a retired generation must
    // not read as "no documents". Check before recovery can remove its files.
    if let Err(error) = refuse_retired_generation(index_path) {
        panic!("{error}");
    }
    let generation = try_recover_generation(index_path)
        .unwrap_or_else(|error| panic!("wal reconciliation: {error}"));
    let (_, _, bm25_path) = storage_paths(index_path, generation.as_ref());
    match probe_path_exists(&bm25_path, "persisted BM25 sidecar") {
        Ok(false) => return 0,
        Ok(true) => {}
        Err(error) => panic!("wal reconciliation: {error}"),
    }
    match Bm25Shard::open(&bm25_path) {
        Ok(store) => u64::from(store.next_doc_id()),
        // Panic, like the rest of the WAL open path: guessing a tip of 0
        // would truncate legitimate document records, and the binary
        // would refuse to serve this sidecar at attach time anyway.
        Err(e) => panic!(
            "wal reconciliation: cannot read {}: {e}",
            bm25_path.display()
        ),
    }
}

/// The WAL-resume table check as a returned error: a log whose
/// manifest records another derived-column declaration or another
/// column table refuses the open naming the difference, instead of
/// reaching `open_wal`'s panic. `NodeServiceImpl::open` runs this
/// before `Self::new`; the panic stays the backstop for the direct
/// constructor, the same split as `declare_derived_columns`.
pub(in crate::node) fn check_wal_tables(
    index: Option<&VectorIndex>,
    config: &NodeConfig,
) -> Result<(), String> {
    if !config.wal {
        return Ok(());
    }
    let Some(index_path) = config.index_path.as_ref() else {
        return Ok(());
    };
    let dir = wal::wal_dir(index_path);
    if !dir.exists() {
        return Ok(());
    }
    let Some((_, gen_dir)) =
        wal::latest_gen(&dir).map_err(|e| format!("scan WAL at {}: {e}", dir.display()))?
    else {
        return Ok(());
    };
    let held = wal::read_manifest(&gen_dir)
        .map_err(|e| format!("read WAL manifest at {}: {e}", gen_dir.display()))?;
    let vector_tip = index.map_or(0, |i| i.len() as u64);
    let doc_tip = persisted_doc_tip(index_path);
    let fresh = wal_manifest(index, config, 0, (vector_tip, doc_tip));
    wal::check_manifest_tables(&held, &fresh).map_err(|e| format!("WAL at {}: {e}", dir.display()))
}

/// Open the shard's WAL at `<index path>.wal/`: resume the newest
/// generation after a restart (truncating any torn tails, continuing the
/// per-file sequences) or start generation 0. A resumed log keeps its own
/// bucket count — the configured `--wal-buckets` only applies at WAL
/// creation. Panics on IO failure, like the BM25 load path in the
/// binary — a shard that cannot log must not silently run unlogged.
///
/// Resume reconciles the log against the applied state first: records at
/// or above the applied tip (`slot_offset + max(vector tip, document
/// tip)`) are truncated, because appends are buffered and a crash can
/// leave the on-disk log ahead of the on-disk indexes — the reopening
/// shard would otherwise re-assign ids the log already holds. The
/// dropped records were never durable-acked (Flush is the durability
/// point).
///
/// A log CREATED over an already-populated shard records the shard's
/// current contents as `preexisting_*` in its manifest: it can serve and
/// recover, but it is not full history and cannot drive a reshard.
///
/// Concurrent first-open is idempotent (see [`wal::open_or_create`]):
/// twin nodes sharing the shard files can cold-start against a shard
/// whose WAL directory does not exist yet, and both end up with a
/// writer onto the same well-formed generation instead of one of them
/// panicking on the mid-create directory.
pub(in crate::node) fn open_wal(
    index: Option<&VectorIndex>,
    config: &NodeConfig,
) -> Option<WalWriter> {
    if !config.wal {
        return None;
    }
    let index_path = config
        .index_path
        .as_ref()
        .expect("wal requires an index path");
    let vector_tip = index.map_or(0, |i| i.len() as u64);
    let doc_tip = persisted_doc_tip(index_path);
    let dir = wal::wal_dir(index_path);
    let cutoff = config.slot_offset + vector_tip.max(doc_tip);
    let fresh = wal_manifest(index, config, 0, (vector_tip, doc_tip));
    let mut writer = wal::open_or_create(&dir, cutoff, fresh.clone())
        .unwrap_or_else(|e| panic!("open WAL at {}: {e}", dir.display()));
    // The log belongs to one collection (docs/collections.md). A manifest
    // from before collections is adopted by the node that opens it, and
    // written; one that names another collection is refused, because a
    // replay of it here would put that dataset's documents into this one.
    let held = writer.manifest().collection.clone();
    if held.is_empty() && !config.collection.is_empty() {
        writer.update_manifest(|m| m.collection = config.collection.clone());
    } else if held != config.collection {
        panic!(
            "WAL at {} belongs to collection {held:?}, but this node serves {:?}; a shard \
             belongs to only one collection",
            dir.display(),
            config.collection
        );
    }
    // The log records the declaration it was written under and the
    // complete column table, empty columns included
    // (docs/derived-columns.md). A manifest from before either is
    // adopted by the node that opens it; one that disagrees is refused,
    // because replaying it here would mix two meanings of one column.
    if let Err(error) = wal::check_manifest_tables(writer.manifest(), &fresh) {
        panic!("WAL at {}: {error}", dir.display());
    }
    writer.update_manifest(|m| {
        if m.derived_fingerprint.is_empty() && m.derived.is_empty() {
            m.derived_fingerprint = fresh.derived_fingerprint.clone();
            m.derived = fresh.derived.clone();
        }
        if m.columns.is_none() {
            m.columns = fresh.columns.clone();
        }
    });
    if writer.manifest().bucket_count != config.wal_buckets {
        eprintln!(
            "wal: --wal-buckets={} ignored; the existing log at {} has bucket_count={}",
            config.wal_buckets,
            writer.dir().display(),
            writer.manifest().bucket_count
        );
    }
    // A resumed generation whose calibration never locked (no records
    // yet) still accepts manifest completion.
    writer.update_manifest(|m| {
        let fresh = wal_manifest(index, config, m.generation, (0, 0));
        if m.dim == 0 {
            m.dim = fresh.dim;
            m.slot_offset = fresh.slot_offset;
        }
        if m.vector_config_format.is_empty() && !fresh.vector_config_format.is_empty() {
            m.vector_backend = fresh.vector_backend;
            m.vector_config_format = fresh.vector_config_format;
            m.vector_config_payload = fresh.vector_config_payload;
        }
    });
    eprintln!("wal: logging to {}", writer.dir().display());
    Some(writer)
}

/// Log `op`, or degrade the shard to unlogged if the append fails.
///
/// The mutation was already applied when this runs (apply-then-log, see
/// [`WalWriter::append`]), so failing the client's request would report a
/// write that in fact happened. Instead the shard keeps serving and the
/// log is retired loudly: the generation directory is renamed `.broken`
/// (so the reshard tool and a restarting node cannot mistake it for
/// history) and the writer is dropped. Per the resharding policy, a
/// shard without a WAL serves fine but can only be rebuilt, never
/// resharded.
pub(crate) fn wal_append_or_degrade(wal_slot: &mut Option<WalWriter>, op: wal_record::Op) {
    let Some(wal) = wal_slot.as_mut() else { return };
    if let Err(e) = wal.append(op) {
        let dir = wal.dir().to_path_buf();
        let broken = dir.with_extension("broken");
        eprintln!(
            "wal: append to {} failed ({e}); retiring the log as {} — this shard continues \
             UNLOGGED and can no longer be resharded from its log (rebuild required)",
            dir.display(),
            broken.display()
        );
        *wal_slot = None;
        if let Err(e) = std::fs::rename(&dir, &broken) {
            eprintln!("wal: could not rename the broken generation: {e}");
        }
    }
}
