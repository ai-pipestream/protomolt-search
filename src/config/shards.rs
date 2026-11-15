//! The vector backend and shard set a process serves.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::{normalize_addr, normalize_analysis_backend, parse_hash_range};
use super::file::{FileConfig, FileShard};
use super::option::{split_list, Options};
use super::{ClusteredTurboVecConfig, DemoConfig, ShardConfig};

/// The clustered TurboVec backend: an external coordinator, an in-process one, or none.
pub(super) fn parse_clustered_turbovec(
    o: Options,
    file: &FileConfig,
) -> Result<Option<ClusteredTurboVecConfig>, String> {
    let section = file.clustered_turbovec.as_ref();
    let endpoint = o.text(
        "turbovec-coordinator",
        section.and_then(|c| c.coordinator.as_deref()),
    );
    let file_nodes = section.and_then(|c| c.nodes.as_ref()).map(|n| n.join(","));
    let nodes = o.text("turbovec-cluster-nodes", file_nodes.as_deref());
    let state = o
        .text(
            "turbovec-cluster-state",
            section.and_then(|c| c.state.as_deref()),
        )
        .map(PathBuf::from);
    let allow_ephemeral = o.switch_or_bare(
        "allow-ephemeral-turbovec-cluster",
        section.and_then(|c| c.allow_ephemeral),
        false,
    );
    match (endpoint, nodes) {
        (Some(_), Some(_)) => Err(
            "clustered TurboVec accepts exactly one of coordinator or nodes, not both".to_string(),
        ),
        (Some(_), None) if state.is_some() || allow_ephemeral => Err(
            "cluster state and allow_ephemeral apply only to an in-process TurboVec coordinator"
                .to_string(),
        ),
        (Some(endpoint), None) => Ok(Some(ClusteredTurboVecConfig::External {
            endpoint: normalize_addr(endpoint),
        })),
        (None, Some(nodes)) => {
            let nodes = split_list(&nodes);
            if nodes.is_empty() {
                return Err("clustered TurboVec node table is empty".to_string());
            }
            if state.is_none() && !allow_ephemeral {
                return Err(
                    "an in-process TurboVec coordinator requires turbovec_cluster_state; set \
                     allow_ephemeral only for tests or demos"
                        .to_string(),
                );
            }
            Ok(Some(ClusteredTurboVecConfig::InProcess {
                nodes,
                state,
                allow_ephemeral,
            }))
        }
        (None, None) if state.is_some() || allow_ephemeral => Err(
            "cluster state or allow_ephemeral was set without clustered TurboVec nodes".to_string(),
        ),
        (None, None) => Ok(None),
    }
}

/// The vector shape new shards are built with.
#[derive(Clone, Copy)]
pub(super) struct VectorShape<'a> {
    pub(super) dim: usize,
    pub(super) bit_width: usize,
    pub(super) backend: &'a str,
}

/// The shard set and the process-wide vocabulary sizing its shards share.
pub(super) struct ShardSet {
    pub(super) shards: Vec<ShardConfig>,
    pub(super) vocab_window_docs: u64,
    pub(super) vocab_top_k: usize,
}

/// What every shard gets unless its own entry says otherwise. The write-ahead
/// log defaults on for persisted shards and is always off for demo shards;
/// vocabulary accumulation defaults off.
struct ShardDefaults<'a> {
    shape: VectorShape<'a>,
    listen: String,
    wal: bool,
    wal_buckets: u32,
    vocab: bool,
}

/// A WAL bucket count: a power of two in `1..=1024`.
fn wal_buckets(value: u32, what: &str) -> Result<u32, String> {
    if !value.is_power_of_two() || value > 1024 {
        return Err(format!(
            "{what}: wal bucket count must be a power of two in 1..=1024, got {value}"
        ));
    }
    Ok(value)
}

fn shard_defaults<'a>(
    o: Options,
    file: &FileConfig,
    shape: VectorShape<'a>,
) -> Result<ShardDefaults<'a>, String> {
    let buckets = o.value_or("wal-buckets", file.wal_buckets, 64)?;
    Ok(ShardDefaults {
        shape,
        listen: o
            .text("node-listen", file.node_listen.as_deref())
            .unwrap_or_else(|| "0.0.0.0:50051".to_string()),
        wal: o.switch("wal", file.wal, true),
        wal_buckets: wal_buckets(buckets, "--wal-buckets")?,
        vocab: o.switch("vocab", file.vocab, false),
    })
}

/// The single shard the command line describes with `--index` or `--demo-vectors`.
fn cli_shard(
    o: Options,
    file: &FileConfig,
    defaults: &ShardDefaults,
    index: Option<String>,
    demo_vectors: Option<usize>,
) -> Result<ShardConfig, String> {
    if index.is_some() && demo_vectors.is_some() {
        return Err("--index and --demo-vectors are mutually exclusive".to_string());
    }
    let listen = defaults
        .listen
        .parse::<SocketAddr>()
        .map_err(|e| super::option::invalid("node-listen", &defaults.listen, e))?;
    let demo = demo_vectors.map(|vectors| DemoConfig {
        vectors,
        dim: defaults.shape.dim,
        bit_width: defaults.shape.bit_width,
    });
    let collection = o.text("collection", None);
    if let Some(name) = &collection {
        crate::collections::validate_name(name).map_err(|e| format!("--collection: {e}"))?;
    }
    Ok(ShardConfig {
        listen,
        vector_backend: defaults.shape.backend.to_string(),
        wal: defaults.wal && demo.is_none(),
        wal_buckets: defaults.wal_buckets,
        vocab: defaults.vocab && demo.is_none(),
        index_path: index.map(PathBuf::from),
        demo,
        slot_offset: o.value_or("slot-offset", file.slot_offset, 0)?,
        analysis_addr: None,
        collection: collection.unwrap_or_default(),
        shard_id: o.text("shard-id", None),
        hash_range: parse_hash_range(
            o.value("hash-lo", None)?,
            o.value("hash-hi", None)?,
            "--hash-lo/--hash-hi",
        )?,
    })
}

/// One `[[shards]]` entry of the config file.
fn file_shard(
    i: usize,
    shard: &FileShard,
    defaults: &ShardDefaults,
) -> Result<ShardConfig, String> {
    let at = format!("shards[{i}]");
    if let Some(name) = &shard.collection {
        crate::collections::validate_name(name).map_err(|e| format!("{at}.collection: {e}"))?;
    }
    let listen = shard
        .listen
        .as_deref()
        .unwrap_or(&defaults.listen)
        .parse::<SocketAddr>()
        .map_err(|e| format!("{at}: invalid listen address: {e}"))?;
    let demo = shard.demo_vectors.map(|vectors| DemoConfig {
        vectors,
        dim: defaults.shape.dim,
        bit_width: defaults.shape.bit_width,
    });
    if shard.index.is_some() == demo.is_some() {
        return Err(format!(
            "{at}: exactly one of index / demo_vectors is required"
        ));
    }
    Ok(ShardConfig {
        listen,
        vector_backend: shard
            .vector_backend
            .clone()
            .unwrap_or_else(|| defaults.shape.backend.to_string()),
        wal: shard.wal.unwrap_or(defaults.wal) && demo.is_none(),
        wal_buckets: wal_buckets(shard.wal_buckets.unwrap_or(defaults.wal_buckets), &at)?,
        vocab: shard.vocab.unwrap_or(defaults.vocab) && demo.is_none(),
        index_path: shard.index.as_ref().map(PathBuf::from),
        demo,
        slot_offset: shard.slot_offset.unwrap_or(0),
        analysis_addr: shard.analysis_addr.clone().map(normalize_analysis_backend),
        collection: shard.collection.clone().unwrap_or_default(),
        shard_id: shard.shard_id.clone(),
        hash_range: parse_hash_range(
            shard.hash_lo,
            shard.hash_hi,
            &format!("{at}.hash_lo/hash_hi"),
        )?,
    })
}

/// The shards this process owns. `--index` or `--demo-vectors` (with
/// `--node-listen` and `--slot-offset`) describes a single shard and
/// overrides the file's `[[shards]]` entirely.
pub(super) fn parse_shards(
    o: Options,
    file: &FileConfig,
    shape: VectorShape,
) -> Result<ShardSet, String> {
    let defaults = shard_defaults(o, file, shape)?;
    let vocab_window_docs = o.value_or(
        "vocab-window-docs",
        file.vocab_window_docs,
        crate::vocab::DEFAULT_WINDOW_DOCS,
    )?;
    if vocab_window_docs == 0 {
        return Err("--vocab-window-docs must be positive".to_string());
    }
    let top_k_default = crate::vocab::HeavyHitters::DEFAULT_CAPACITY;
    let vocab_top_k = o.value_or("vocab-top-k", file.vocab_top_k, top_k_default)?;
    if vocab_top_k == 0 {
        return Err("--vocab-top-k must be positive".to_string());
    }
    let index = o.text("index", file.index.as_deref());
    let demo_vectors = o.value("demo-vectors", file.demo_vectors)?;
    let shards = if index.is_some() || demo_vectors.is_some() {
        vec![cli_shard(o, file, &defaults, index, demo_vectors)?]
    } else {
        let entries = file.shards.iter().enumerate();
        entries
            .map(|(i, shard)| file_shard(i, shard, &defaults))
            .collect::<Result<_, String>>()?
    };
    Ok(ShardSet {
        shards,
        vocab_window_docs,
        vocab_top_k,
    })
}
