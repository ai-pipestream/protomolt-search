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

/// The rows the shard's catalog has sealed: the applied tip of its log.
/// Read straight from the manifest so WAL reconciliation does not depend
/// on attachment order. Panics, like the rest of the WAL open path, on a
/// manifest it cannot read: guessing a tip of 0 would truncate every
/// sealed record from the log.
fn catalog_row_tip(index_path: &Path) -> u64 {
    let manifest = crate::segments::SegmentCatalog::read_manifest(&segments_root(index_path))
        .unwrap_or_else(|error| panic!("wal reconciliation: {error}"));
    manifest
        .map(|manifest| {
            manifest
                .segments
                .iter()
                .map(|segment| segment.base_label.saturating_add(segment.rows))
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0)
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
    let fresh = wal_manifest(index, config, 0, (vector_tip, catalog_row_tip(index_path)));
    wal::check_manifest_tables(&held, &fresh).map_err(|e| format!("WAL at {}: {e}", dir.display()))
}

/// Open or create the shard's WAL at `<index_path>.wal/`, truncating buffered
/// records at or above the persisted index tip (`slot_offset + max(vector_tip, doc_tip)`)
/// and recording `preexisting_*` counts when initialized over an existing shard.
/// Concurrent cold-starts are idempotent via [`wal::open_or_create`]; panics on I/O failure.
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
    let doc_tip = catalog_row_tip(index_path);
    let dir = wal::wal_dir(index_path);
    let cutoff = config.slot_offset + vector_tip.max(doc_tip);
    let fresh = wal_manifest(index, config, 0, (vector_tip, doc_tip));
    let mut writer = wal::open_or_create(&dir, cutoff, fresh.clone())
        .unwrap_or_else(|e| panic!("open WAL at {}: {e}", dir.display()));
    // Adopt empty collection manifests; panic if the log belongs to another collection.
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
    // Verify derived-column declaration and column-table compatibility, adopting unset fields.
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
