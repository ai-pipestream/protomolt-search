//! Startup validation of an embedded configuration and the storage paths
//! it names.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{
    EmbeddedDocumentCatalogConfig, EmbeddedError, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use crate::analyzer::NATIVE_ANALYSIS_BACKEND;
use crate::node::{
    bm25_sidecar_path, exact_vector_sidecar_path, generation_dir, live_docs_sidecar_path,
};

pub(super) fn validate_config(config: &mut EmbeddedSearchConfig) -> Result<(), EmbeddedError> {
    validate_limits(config)?;
    validate_single_collection(config)?;

    let catalog_path = config
        .document_catalog
        .as_ref()
        .and_then(|c| c.path.as_ref())
        .map(|path| canonical_storage_path(path))
        .transpose()?;
    let mut paths = HashSet::new();
    for (shard, shard_config) in config.shards.iter_mut().enumerate() {
        if let Some(catalog) = &config.document_catalog {
            validate_catalog_separation(shard, shard_config, catalog, catalog_path.as_deref())?;
        }
        normalize_analysis_backend(shard, shard_config)?;
        if shard_config.node.wal && shard_config.node.index_path.is_none() {
            return Err(EmbeddedError::InvalidConfig(format!(
                "embedded shard {shard} enables WAL without an index path"
            )));
        }
        if let Some(path) = shard_config.node.index_path.as_ref() {
            if !paths.insert(path.clone()) {
                return Err(EmbeddedError::InvalidConfig(format!(
                    "multiple embedded shards use the same index path {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_limits(config: &EmbeddedSearchConfig) -> Result<(), EmbeddedError> {
    if config.shards.is_empty() {
        return Err(EmbeddedError::InvalidConfig(
            "embedded search requires at least one shard".to_string(),
        ));
    }
    if config.max_k == 0 {
        return Err(EmbeddedError::InvalidConfig(
            "embedded max_k must be positive".to_string(),
        ));
    }
    if config.max_rerank_bytes == 0 {
        return Err(EmbeddedError::InvalidConfig(
            "embedded max_rerank_bytes must be positive".to_string(),
        ));
    }
    Ok(())
}

fn validate_single_collection(config: &EmbeddedSearchConfig) -> Result<(), EmbeddedError> {
    let collection = &config.shards[0].node.collection;
    if !collection.is_empty() {
        crate::collections::validate_name(collection).map_err(EmbeddedError::InvalidConfig)?;
    }
    for (shard, shard_config) in config.shards.iter().enumerate() {
        if &shard_config.node.collection != collection {
            return Err(EmbeddedError::InvalidConfig(format!(
                "embedded shard {shard} collection differs; one runtime serves one collection"
            )));
        }
    }
    Ok(())
}

/// Refuses a shard whose collection differs from the catalog's, or whose
/// storage artifacts contain the catalog path.
fn validate_catalog_separation(
    shard: usize,
    shard_config: &EmbeddedShardConfig,
    catalog: &EmbeddedDocumentCatalogConfig,
    catalog_path: Option<&Path>,
) -> Result<(), EmbeddedError> {
    if shard_config.node.collection != catalog.collection {
        return Err(EmbeddedError::InvalidConfig(format!(
            "embedded shard {shard} collection differs from the document catalog"
        )));
    }
    if let (Some(path), Some(index)) = (catalog_path, &shard_config.node.index_path) {
        for artifact in [
            index.clone(),
            bm25_sidecar_path(index),
            crate::node::bm25_build_dir(&bm25_sidecar_path(index)),
            exact_vector_sidecar_path(index),
            live_docs_sidecar_path(index),
            generation_dir(index),
            crate::node::segments_root(index),
            crate::compaction::default_work_dir(index),
            crate::compaction::marker_path(index),
            crate::wal::wal_dir(index),
        ] {
            if path.starts_with(canonical_storage_path(&artifact)?) {
                return Err(EmbeddedError::InvalidConfig(
                    "document catalog path overlaps shard storage".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Resolves an absent or native analysis backend to the native one and
/// refuses any other address.
fn normalize_analysis_backend(
    shard: usize,
    shard_config: &mut EmbeddedShardConfig,
) -> Result<(), EmbeddedError> {
    match shard_config.node.analysis_addr.as_deref() {
        None | Some("native") | Some("native://") => {
            shard_config.node.analysis_addr = Some(NATIVE_ANALYSIS_BACKEND.to_string());
        }
        Some(address) => {
            return Err(EmbeddedError::InvalidConfig(format!(
                "embedded shard {shard} analysis backend {address:?} is not local; only native is allowed"
            )));
        }
    }
    Ok(())
}

fn canonical_storage_path(path: &Path) -> Result<PathBuf, EmbeddedError> {
    let result = if path.exists() {
        path.canonicalize()
    } else {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        parent
            .canonicalize()
            .map(|parent| parent.join(path.file_name().unwrap_or_default()))
    };
    result.map_err(|error| {
        EmbeddedError::InvalidConfig(format!(
            "resolve storage path {} (parent must exist): {error}",
            path.display()
        ))
    })
}

pub(super) fn first_existing_artifact(index_path: &Path) -> Option<PathBuf> {
    [
        index_path.to_path_buf(),
        bm25_sidecar_path(index_path),
        exact_vector_sidecar_path(index_path),
        live_docs_sidecar_path(index_path),
        generation_dir(index_path),
        crate::wal::wal_dir(index_path),
    ]
    .into_iter()
    .find(|path| path.exists())
}
