//! The vector backend and shard set a process serves.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::{normalize_addrs, normalize_analysis_backend, parse_hash_range};
use super::file::{FileConfig, FileShard};
use super::option::{split_list, Options};
use super::{ClusteredTurboVecConfig, DemoConfig, ShardConfig};

/// The clustered TurboVec backend: an external coordinator, an in-process one, or none.
pub(super) fn parse_clustered_turbovec(
    o: Options<'_>,
    file: &FileConfig,
) -> Result<Option<ClusteredTurboVecConfig>, String> {
    let in_file = file.clustered_turbovec.as_ref();
    let endpoint = o.text(
        "turbovec-coordinator",
        in_file.and_then(|config| config.coordinator.as_deref()),
    );
    let nodes = o.list(
        "turbovec-cluster-nodes",
        in_file.and_then(|config| config.nodes.as_ref()),
    );
    let nodes: Vec<String> = nodes.iter().flat_map(|entry| split_list(entry)).collect();
    let nodes_given = o.given("turbovec-cluster-nodes").is_some()
        || in_file.is_some_and(|config| config.nodes.is_some());
    let state = o
        .text(
            "turbovec-cluster-state",
            in_file.and_then(|config| config.state.as_deref()),
        )
        .map(PathBuf::from);
    let allow_ephemeral = o.switch_or_bare(
        "allow-ephemeral-turbovec-cluster",
        in_file.and_then(|config| config.allow_ephemeral),
        false,
    );
    match (endpoint, nodes_given) {
        (Some(_), true) => Err(
            "clustered TurboVec accepts exactly one of coordinator or nodes, not both".to_string(),
        ),
        (Some(endpoint), false) => {
            if state.is_some() || allow_ephemeral {
                return Err(
                    "cluster state and allow_ephemeral apply only to an in-process TurboVec coordinator"
                        .to_string(),
                );
            }
            Ok(Some(ClusteredTurboVecConfig::External {
                endpoint: normalize_addrs(vec![endpoint]).remove(0),
            }))
        }
        (None, true) => {
            if nodes.is_empty() {
                return Err("clustered TurboVec node table is empty".to_string());
            }
            if state.is_none() && !allow_ephemeral {
                return Err(
                    "an in-process TurboVec coordinator requires turbovec_cluster_state; set allow_ephemeral only for tests or demos"
                        .to_string(),
                );
            }
            Ok(Some(ClusteredTurboVecConfig::InProcess {
                nodes,
                state,
                allow_ephemeral,
            }))
        }
        (None, false) => {
            if state.is_some() || allow_ephemeral {
                return Err(
                    "cluster state or allow_ephemeral was set without clustered TurboVec nodes"
                        .to_string(),
                );
            }
            Ok(None)
        }
    }
}

/// The shard set and the process-wide vocabulary sizing its shards share.
pub(super) struct ShardSet {
    pub(super) shards: Vec<ShardConfig>,
    pub(super) vocab_window_docs: u64,
    pub(super) vocab_top_k: usize,
}

/// The vector shape every shard of the process shares.
pub(super) struct VectorShape<'a> {
    pub(super) dim: usize,
    pub(super) bit_width: usize,
    pub(super) backend: &'a str,
}

/// Process-wide shard defaults a `[[shards]]` entry may override.
struct ShardDefaults<'a> {
    listen: String,
    wal: bool,
    wal_buckets: u32,
    vocab: bool,
    shape: VectorShape<'a>,
}

fn wal_buckets(count: u32, what: &str) -> Result<u32, String> {
    if !count.is_power_of_two() || count == 0 || count > 1024 {
        return Err(format!(
            "{what}: wal bucket count must be a power of two in 1..=1024, got {count}"
        ));
    }
    Ok(count)
}

/// The shards this process owns: one from the command line, or the file's `[[shards]]`.
pub(super) fn parse_shards(
    o: Options<'_>,
    file: &FileConfig,
    shape: VectorShape<'_>,
) -> Result<ShardSet, String> {
    // The log is on by default for persisted shards and always off for
    // demo shards. Its bucket count caps cheap split granularity.
    let defaults = ShardDefaults {
        listen: o
            .text("node-listen", file.node_listen.as_deref())
            .unwrap_or_else(|| "0.0.0.0:50051".to_string()),
        wal: o.switch("wal", file.wal, true),
        wal_buckets: wal_buckets(
            o.value("wal-buckets", file.wal_buckets)?.unwrap_or(64),
            "--wal-buckets",
        )?,
        vocab: o.switch("vocab", file.vocab, false),
        shape,
    };
    let vocab_window_docs = o
        .value("vocab-window-docs", file.vocab_window_docs)?
        .unwrap_or(crate::vocab::DEFAULT_WINDOW_DOCS);
    if vocab_window_docs == 0 {
        return Err("vocab window docs must be positive".to_string());
    }
    let vocab_top_k = o
        .value("vocab-top-k", file.vocab_top_k)?
        .unwrap_or(crate::vocab::HeavyHitters::DEFAULT_CAPACITY);
    if vocab_top_k == 0 {
        return Err("vocab top-K must be positive".to_string());
    }
    for (i, shard) in file.shards.iter().enumerate() {
        if let Some(name) = &shard.collection {
            crate::collections::validate_name(name)
                .map_err(|e| format!("shards[{i}].collection: {e}"))?;
        }
    }
    let shards = match command_line_shard(o, file, &defaults)? {
        Some(shard) => vec![shard],
        None => file
            .shards
            .iter()
            .enumerate()
            .map(|(i, shard)| file_shard(i, shard, &defaults))
            .collect::<Result<_, String>>()?,
    };
    Ok(ShardSet {
        shards,
        vocab_window_docs,
        vocab_top_k,
    })
}

/// The single shard `--index` or `--demo-vectors` describes; it replaces
/// the file's `[[shards]]` entirely.
fn command_line_shard(
    o: Options<'_>,
    file: &FileConfig,
    defaults: &ShardDefaults<'_>,
) -> Result<Option<ShardConfig>, String> {
    let index = o.text("index", file.index.as_deref());
    let demo_vectors = o.value::<usize>("demo-vectors", file.demo_vectors)?;
    let slot_offset = o.value("slot-offset", file.slot_offset)?.unwrap_or(0);
    let collection = o.given("collection");
    if let Some(name) = &collection {
        crate::collections::validate_name(name).map_err(|e| format!("--collection: {e}"))?;
    }
    if index.is_none() && demo_vectors.is_none() {
        return Ok(None);
    }
    if index.is_some() && demo_vectors.is_some() {
        return Err("--index and --demo-vectors are mutually exclusive".to_string());
    }
    let listen = defaults
        .listen
        .parse::<SocketAddr>()
        .map_err(|e| format!("invalid node listen address: {e}"))?;
    let demo = demo_vectors.map(|vectors| DemoConfig {
        vectors,
        dim: defaults.shape.dim,
        bit_width: defaults.shape.bit_width,
    });
    Ok(Some(ShardConfig {
        listen,
        vector_backend: defaults.shape.backend.to_string(),
        wal: defaults.wal && demo.is_none(),
        wal_buckets: defaults.wal_buckets,
        vocab: defaults.vocab && demo.is_none(),
        index_path: index.map(PathBuf::from),
        demo,
        slot_offset,
        analysis_addr: None,
        collection: collection.unwrap_or_default(),
        shard_id: o.given("shard-id"),
        hash_range: parse_hash_range(
            o.value::<u64>("hash-lo", None)?,
            o.value::<u64>("hash-hi", None)?,
            "--hash-lo/--hash-hi",
        )?,
    }))
}

/// One `[[shards]]` entry over the process defaults.
fn file_shard(
    i: usize,
    shard: &FileShard,
    defaults: &ShardDefaults<'_>,
) -> Result<ShardConfig, String> {
    let listen = shard
        .listen
        .clone()
        .unwrap_or_else(|| defaults.listen.clone())
        .parse::<SocketAddr>()
        .map_err(|e| format!("shards[{i}]: invalid listen address: {e}"))?;
    let demo = shard.demo_vectors.map(|vectors| DemoConfig {
        vectors,
        dim: defaults.shape.dim,
        bit_width: defaults.shape.bit_width,
    });
    if shard.index.is_some() == demo.is_some() {
        return Err(format!(
            "shards[{i}]: exactly one of index / demo_vectors is required"
        ));
    }
    Ok(ShardConfig {
        listen,
        vector_backend: shard
            .vector_backend
            .clone()
            .unwrap_or_else(|| defaults.shape.backend.to_string()),
        wal: shard.wal.unwrap_or(defaults.wal) && demo.is_none(),
        wal_buckets: wal_buckets(
            shard.wal_buckets.unwrap_or(defaults.wal_buckets),
            &format!("shards[{i}]"),
        )?,
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
            &format!("shards[{i}].hash_lo/hash_hi"),
        )?,
    })
}
