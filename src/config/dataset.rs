//! Per-dataset query settings, segment storage settings of new shards, and the
//! named collections that carry their own dataset settings.

use std::collections::BTreeMap;
use std::path::PathBuf;

use super::args::{normalize_addrs, normalize_analysis_backend};
use super::file::{FileCollection, FileConfig};
use super::option::Options;
use super::shard_map::load_shard_map;
use super::topology::{map_node_addrs, map_replica_addrs};
use super::{CollectionConfig, Config};

/// Analysis, BM25 parameters, and the query-time policy files.
pub(super) struct Lexical {
    pub(super) analysis_addr: Option<String>,
    pub(super) bm25_k1: f32,
    pub(super) bm25_b: f32,
    pub(super) dense_quality_profile: Option<PathBuf>,
    pub(super) synonyms: Option<PathBuf>,
    pub(super) dense_execution_policy: Option<PathBuf>,
}

/// How a persisted shard seals and maps its segments.
pub(super) struct Storage {
    pub(super) seal_tail_docs: u32,
    pub(super) vector_mmap: bool,
}

pub(super) fn resolve_lexical(o: Options, file: &FileConfig) -> Result<Lexical, String> {
    Ok(Lexical {
        analysis_addr: o
            .text("analysis-addr", file.analysis_addr.as_deref())
            .map(normalize_analysis_backend),
        bm25_k1: o.value_or("bm25-k1", file.bm25_k1, 1.2)?,
        bm25_b: o.value_or("bm25-b", file.bm25_b, 0.75)?,
        dense_quality_profile: o.path(
            "dense-quality-profile",
            file.dense_quality_profile.as_deref(),
        ),
        synonyms: o.path("synonyms", file.synonyms.as_deref()),
        dense_execution_policy: o.path(
            "dense-execution-policy",
            file.dense_execution_policy.as_deref(),
        ),
    })
}

pub(super) fn resolve_storage(o: Options, file: &FileConfig) -> Result<Storage, String> {
    let vector_mmap = match o.given("vector-mmap").as_deref() {
        None => file.vector_mmap.unwrap_or(true),
        Some("true") => true,
        Some("false") => false,
        Some(other) => {
            return Err(format!(
                "--vector-mmap={other:?} is not a boolean; use true (the default) or false"
            ))
        }
    };
    Ok(Storage {
        seal_tail_docs: o.value_or("seal-tail-docs", file.seal_tail_docs, 500_000)?,
        vector_mmap,
    })
}

/// Named collections replace the top-level dataset: its fan-out set, its
/// control state, its replica cursors, and the clustered vector backend.
fn refuse_top_level_dataset(config: &Config) -> Result<(), String> {
    let conflict = if !config.node_addrs.is_empty() {
        "`collections` replaces --nodes / --shard-map: put each collection's nodes or \
         shard_map on the collection"
    } else if config.control_state_path.is_some() {
        "`collections` replaces --control-state: put each collection's control_state on \
         the collection"
    } else if config.replica_state_path.is_some() {
        "`collections` replaces --replica-state: put each collection's replica_state on \
         the collection"
    } else if config.clustered_turbovec.is_some() {
        "clustered TurboVec serves one dataset and is not yet configurable per \
         collection; drop `collections` or the clustered backend"
    } else {
        return Ok(());
    };
    Err(conflict.to_string())
}

/// One collection: its fan-out set, replicas, and durable state paths, plus
/// its own dataset settings, falling back to the top-level ones.
fn collection(at: &str, c: &FileCollection, config: &Config) -> Result<CollectionConfig, String> {
    let shard_map_path = c.shard_map.as_ref().map(PathBuf::from);
    if shard_map_path.is_some() && c.nodes.is_some() {
        return Err(format!("{at}: shard_map replaces nodes; give only one"));
    }
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let (node_addrs, replica_addrs) = match &shard_map {
        Some(map) => (map_node_addrs(map), map_replica_addrs(map)),
        None => (
            normalize_addrs(c.nodes.clone().unwrap_or_default()),
            Vec::new(),
        ),
    };
    if node_addrs.is_empty() {
        return Err(format!("{at}: a collection needs nodes or a shard_map"));
    }
    let control_state_path = c.control_state.as_ref().map(PathBuf::from);
    if control_state_path.is_some() && shard_map.is_none() {
        return Err(format!(
            "{at}: control_state requires a generation-written shard_map"
        ));
    }
    let replica_state_path = c.replica_state.as_ref().map(PathBuf::from);
    let syncs = config.replica_sync_ms > 0 && replica_addrs.iter().any(Option::is_some);
    if syncs && replica_state_path.is_none() {
        return Err(format!(
            "{at}: automatic replica sync needs replica_state on the collection"
        ));
    }
    if config.shard_map_reload_ms > 0 && shard_map_path.is_none() {
        return Err(format!(
            "{at}: --shard-map-reload-ms needs a shard_map on every collection"
        ));
    }
    let own_path = |value: &Option<String>, fallback: &Option<PathBuf>| {
        value
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| fallback.clone())
    };
    Ok(CollectionConfig {
        name: c.name.clone(),
        node_addrs,
        replica_addrs,
        shard_map,
        shard_map_path,
        analysis_addr: c
            .analysis_addr
            .clone()
            .map(normalize_analysis_backend)
            .or_else(|| config.analysis_addr.clone()),
        bm25_k1: c.bm25_k1.unwrap_or(config.bm25_k1),
        bm25_b: c.bm25_b.unwrap_or(config.bm25_b),
        dense_quality_profile: own_path(&c.dense_quality_profile, &config.dense_quality_profile),
        synonyms: own_path(&c.synonyms, &config.synonyms),
        dense_execution_policy: own_path(&c.dense_execution_policy, &config.dense_execution_policy),
        replica_state_path,
        control_state_path,
    })
}

/// The named collections: each carries its own dataset settings, so the
/// top-level dataset options must then be absent.
pub(super) fn resolve_collections(
    file: &FileConfig,
    config: &Config,
) -> Result<Vec<CollectionConfig>, String> {
    if file.collections.is_empty() {
        return Ok(Vec::new());
    }
    refuse_top_level_dataset(config)?;
    let mut collections: Vec<CollectionConfig> = Vec::with_capacity(file.collections.len());
    let mut owners: BTreeMap<String, String> = BTreeMap::new();
    for (i, c) in file.collections.iter().enumerate() {
        let at = format!("collections[{i}]");
        crate::collections::validate_name(&c.name).map_err(|e| format!("{at}: {e}"))?;
        if collections.iter().any(|known| known.name == c.name) {
            return Err(format!("{at}: collection {:?} is declared twice", c.name));
        }
        let collection = collection(&at, c, config)?;
        for addr in &collection.node_addrs {
            if let Some(other) = owners.insert(addr.clone(), c.name.clone()) {
                return Err(format!(
                    "{at}: node {addr} is already listed under collection {other:?}; a shard \
                     belongs to only one collection"
                ));
            }
        }
        collections.push(collection);
    }
    Ok(collections)
}
