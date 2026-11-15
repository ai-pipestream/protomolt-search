//! The unnamed dataset's lexical settings, the on-disk layout, and the
//! named collections a coordinator serves.

use std::collections::BTreeMap;
use std::path::PathBuf;

use super::args::{normalize_addrs, normalize_analysis_backend};
use super::file::{FileCollection, FileConfig};
use super::option::Options;
use super::shard_map::load_shard_map;
use super::topology::replicas_of;
use super::{CollectionConfig, Config};

/// Analysis, BM25 parameters, and the profile files of the unnamed dataset.
pub(super) struct Lexical {
    pub(super) analysis_addr: Option<String>,
    pub(super) bm25_k1: f32,
    pub(super) bm25_b: f32,
    pub(super) dense_quality_profile: Option<PathBuf>,
    pub(super) synonyms: Option<PathBuf>,
    pub(super) dense_execution_policy: Option<PathBuf>,
}

pub(super) fn resolve_lexical(o: Options<'_>, file: &FileConfig) -> Result<Lexical, String> {
    Ok(Lexical {
        analysis_addr: o
            .text("analysis-addr", file.analysis_addr.as_deref())
            .map(normalize_analysis_backend),
        bm25_k1: o.value("bm25-k1", file.bm25_k1)?.unwrap_or(1.2),
        bm25_b: o.value("bm25-b", file.bm25_b)?.unwrap_or(0.75),
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

/// How a shard lays its index out on disk.
pub(super) struct Storage {
    pub(super) layout: crate::node::Layout,
    pub(super) seal_tail_docs: u32,
    pub(super) vector_mmap: bool,
}

pub(super) fn resolve_storage(o: Options<'_>, file: &FileConfig) -> Result<Storage, String> {
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
    let layout = match o.text("layout", file.layout.as_deref()).as_deref() {
        None | Some("segments") => crate::node::Layout::Segments,
        Some("single-image") => crate::node::Layout::SingleImage,
        Some(other) => {
            return Err(format!(
                "--layout={other:?} is not a layout; use segments (the default) or single-image"
            ))
        }
    };
    Ok(Storage {
        layout,
        seal_tail_docs: o
            .value("seal-tail-docs", file.seal_tail_docs)?
            .unwrap_or(500_000),
        vector_mmap,
    })
}

/// Named collections (docs/collections.md). Each carries its own dataset
/// settings, so the top-level ones that name a dataset must be absent.
pub(super) fn resolve_collections(
    file: &FileConfig,
    config: &Config,
) -> Result<Vec<CollectionConfig>, String> {
    if file.collections.is_empty() {
        return Ok(Vec::new());
    }
    let replaced = [
        (!config.node_addrs.is_empty(), "`collections` replaces --nodes / --shard-map: put each collection's nodes or shard_map on the collection"),
        (config.control_state_path.is_some(), "`collections` replaces --control-state: put each collection's control_state on the collection"),
        (config.replica_state_path.is_some(), "`collections` replaces --replica-state: put each collection's replica_state on the collection"),
        (config.clustered_turbovec.is_some(), "clustered TurboVec serves one dataset and is not yet configurable per collection; drop `collections` or the clustered backend"),
    ];
    if let Some((_, message)) = replaced.iter().find(|(set, _)| *set) {
        return Err((*message).to_string());
    }
    let mut collections: Vec<CollectionConfig> = Vec::with_capacity(file.collections.len());
    let mut owners: BTreeMap<String, String> = BTreeMap::new();
    for (i, entry) in file.collections.iter().enumerate() {
        let at = format!("collections[{i}]");
        crate::collections::validate_name(&entry.name).map_err(|e| format!("{at}: {e}"))?;
        if collections.iter().any(|known| known.name == entry.name) {
            return Err(format!(
                "{at}: collection {:?} is declared twice",
                entry.name
            ));
        }
        let collection = one_collection(&at, entry, config)?;
        for addr in &collection.node_addrs {
            if let Some(other) = owners.insert(addr.clone(), entry.name.clone()) {
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

/// One `[[collections]]` entry over the unnamed dataset's settings.
fn one_collection(
    at: &str,
    entry: &FileCollection,
    config: &Config,
) -> Result<CollectionConfig, String> {
    let shard_map_path = entry.shard_map.as_ref().map(PathBuf::from);
    if shard_map_path.is_some() && entry.nodes.is_some() {
        return Err(format!("{at}: shard_map replaces nodes; give only one"));
    }
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let node_addrs = match &shard_map {
        Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
        None => normalize_addrs(entry.nodes.clone().unwrap_or_default()),
    };
    if node_addrs.is_empty() {
        return Err(format!("{at}: a collection needs nodes or a shard_map"));
    }
    let replica_addrs = shard_map.as_ref().map(replicas_of).unwrap_or_default();
    let control_state_path = entry.control_state.as_ref().map(PathBuf::from);
    if control_state_path.is_some() && shard_map.is_none() {
        return Err(format!(
            "{at}: control_state requires a generation-written shard_map"
        ));
    }
    let replica_state_path = entry.replica_state.as_ref().map(PathBuf::from);
    if config.replica_sync_ms > 0
        && replica_addrs.iter().any(Option::is_some)
        && replica_state_path.is_none()
    {
        return Err(format!(
            "{at}: automatic replica sync needs replica_state on the collection"
        ));
    }
    if config.shard_map_reload_ms > 0 && shard_map_path.is_none() {
        return Err(format!(
            "{at}: --shard-map-reload-ms needs a shard_map on every collection"
        ));
    }
    let or_path = |own: &Option<String>, shared: &Option<PathBuf>| {
        own.as_ref().map(PathBuf::from).or_else(|| shared.clone())
    };
    Ok(CollectionConfig {
        name: entry.name.clone(),
        node_addrs,
        replica_addrs,
        shard_map,
        shard_map_path,
        analysis_addr: entry
            .analysis_addr
            .clone()
            .map(normalize_analysis_backend)
            .or_else(|| config.analysis_addr.clone()),
        bm25_k1: entry.bm25_k1.unwrap_or(config.bm25_k1),
        bm25_b: entry.bm25_b.unwrap_or(config.bm25_b),
        dense_quality_profile: or_path(&entry.dense_quality_profile, &config.dense_quality_profile),
        synonyms: or_path(&entry.synonyms, &config.synonyms),
        dense_execution_policy: or_path(
            &entry.dense_execution_policy,
            &config.dense_execution_policy,
        ),
        replica_state_path,
        control_state_path,
    })
}
