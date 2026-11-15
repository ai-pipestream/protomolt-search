//! The vector backend and shard set a process serves.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::{
    flag_present, normalize_addrs, normalize_analysis_backend, opt, parse_env_bool,
    parse_hash_range,
};
use super::file::FileConfig;
use super::{ClusteredTurboVecConfig, DemoConfig, ShardConfig};

/// The clustered TurboVec backend: an external coordinator, an in-process one, or none.
pub(super) fn parse_clustered_turbovec(
    args: &[String],
    file: &FileConfig,
) -> Result<Option<ClusteredTurboVecConfig>, String> {
    let clustered_file = file.clustered_turbovec.as_ref();
    let clustered_endpoint = opt(
        args,
        "turbovec-coordinator",
        "PIPESTREAM_SEARCH_COORDINATOR",
        clustered_file.and_then(|config| config.coordinator.as_deref()),
    );
    let clustered_nodes = opt(
        args,
        "turbovec-cluster-nodes",
        "PIPESTREAM_SEARCH_CLUSTER_NODES",
        clustered_file
            .and_then(|config| config.nodes.as_ref())
            .map(|nodes| nodes.join(","))
            .as_deref(),
    );
    if clustered_endpoint.is_some() && clustered_nodes.is_some() {
        return Err(
            "clustered TurboVec accepts exactly one of coordinator or nodes, not both".to_string(),
        );
    }
    let clustered_state = opt(
        args,
        "turbovec-cluster-state",
        "PIPESTREAM_SEARCH_CLUSTER_STATE",
        clustered_file.and_then(|config| config.state.as_deref()),
    )
    .map(PathBuf::from);
    let clustered_allow_ephemeral = flag_present(args, "allow-ephemeral-turbovec-cluster")
        || match opt(
            args,
            "allow-ephemeral-turbovec-cluster",
            "PIPESTREAM_SEARCH_ALLOW_EPHEMERAL_CLUSTER",
            None,
        ) {
            Some(value) => parse_env_bool(&value),
            None => clustered_file
                .and_then(|config| config.allow_ephemeral)
                .unwrap_or(false),
        };
    let clustered_turbovec = match (clustered_endpoint, clustered_nodes) {
        (Some(endpoint), None) => {
            if clustered_state.is_some() || clustered_allow_ephemeral {
                return Err(
                    "cluster state and allow_ephemeral apply only to an in-process TurboVec coordinator"
                        .to_string(),
                );
            }
            Some(ClusteredTurboVecConfig::External {
                endpoint: normalize_addrs(vec![endpoint]).remove(0),
            })
        }
        (None, Some(nodes)) => {
            let nodes: Vec<String> = nodes
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect();
            if nodes.is_empty() {
                return Err("clustered TurboVec node table is empty".to_string());
            }
            if clustered_state.is_none() && !clustered_allow_ephemeral {
                return Err(
                    "an in-process TurboVec coordinator requires turbovec_cluster_state; set allow_ephemeral only for tests or demos"
                        .to_string(),
                );
            }
            Some(ClusteredTurboVecConfig::InProcess {
                nodes,
                state: clustered_state,
                allow_ephemeral: clustered_allow_ephemeral,
            })
        }
        (None, None) => {
            if clustered_state.is_some() || clustered_allow_ephemeral {
                return Err(
                    "cluster state or allow_ephemeral was set without clustered TurboVec nodes"
                        .to_string(),
                );
            }
            None
        }
        (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
    };
    Ok(clustered_turbovec)
}

/// The shard set and the process-wide vocabulary sizing its shards share.
pub(super) struct ShardSet {
    pub(super) shards: Vec<ShardConfig>,
    pub(super) vocab_window_docs: u64,
    pub(super) vocab_top_k: usize,
}

/// The shards this process owns: one from the command line, or the file's `[[shards]]`.
pub(super) fn parse_shards(
    args: &[String],
    file: &FileConfig,
    dim: usize,
    bit_width: usize,
    vector_backend: &str,
) -> Result<ShardSet, String> {
    let vector_backend = vector_backend.to_string();
    // Shard set. CLI --index/--demo-vectors (with --node-listen and
    // --slot-offset) describes a single shard and overrides the file's
    // [[shards]] entirely; otherwise the file's shards are used.
    let node_listen_default = opt(
        args,
        "node-listen",
        "PIPESTREAM_SEARCH_NODE_LISTEN",
        file.node_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50051".to_string());
    let cli_index = opt(
        args,
        "index",
        "PIPESTREAM_SEARCH_INDEX",
        file.index.as_deref(),
    );
    let cli_demo = opt(
        args,
        "demo-vectors",
        "PIPESTREAM_SEARCH_DEMO_VECTORS",
        file.demo_vectors.map(|d| d.to_string()).as_deref(),
    );
    let cli_offset = opt(
        args,
        "slot-offset",
        "PIPESTREAM_SEARCH_SLOT_OFFSET",
        file.slot_offset.map(|o| o.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "0".to_string())
    .parse::<u64>()
    .map_err(|e| format!("invalid slot offset: {e}"))?;

    // Write-ahead log: default on for persisted shards, always off for
    // demo shards (they have no index path to log next to). Bucket count
    // is a power of two (max 1024) and caps cheap split granularity.
    let wal_default = match opt(args, "wal", "PIPESTREAM_SEARCH_WAL", None) {
        Some(s) => parse_env_bool(&s),
        None => file.wal.unwrap_or(true),
    };
    let wal_buckets_default = opt(
        args,
        "wal-buckets",
        "PIPESTREAM_SEARCH_WAL_BUCKETS",
        file.wal_buckets.map(|b| b.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid wal bucket count: {e}"))
    })
    .transpose()?
    .unwrap_or(64);
    let parse_buckets = |b: u32, what: &str| -> Result<u32, String> {
        if !b.is_power_of_two() || b == 0 || b > 1024 {
            return Err(format!(
                "{what}: wal bucket count must be a power of two in 1..=1024, got {b}"
            ));
        }
        Ok(b)
    };
    let wal_buckets_default = parse_buckets(wal_buckets_default, "--wal-buckets")?;

    // Vocabulary accumulation: default OFF (zero overhead); per-shard file
    // entries may override the process default. Window and top-K sizing is
    // process-wide (see docs/VOCABULARY-INDEX.md).
    let vocab_default = match opt(args, "vocab", "PIPESTREAM_SEARCH_VOCAB", None) {
        Some(s) => parse_env_bool(&s),
        None => file.vocab.unwrap_or(false),
    };
    let vocab_window_docs = opt(
        args,
        "vocab-window-docs",
        "PIPESTREAM_SEARCH_VOCAB_WINDOW_DOCS",
        file.vocab_window_docs.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid vocab window docs: {e}"))
    })
    .transpose()?
    .unwrap_or(crate::vocab::DEFAULT_WINDOW_DOCS);
    if vocab_window_docs == 0 {
        return Err("vocab window docs must be positive".to_string());
    }
    let vocab_top_k = opt(
        args,
        "vocab-top-k",
        "PIPESTREAM_SEARCH_VOCAB_TOP_K",
        file.vocab_top_k.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid vocab top-K: {e}"))
    })
    .transpose()?
    .unwrap_or(crate::vocab::HeavyHitters::DEFAULT_CAPACITY);
    if vocab_top_k == 0 {
        return Err("vocab top-K must be positive".to_string());
    }

    let cli_collection = opt(args, "collection", "PIPESTREAM_SEARCH_COLLECTION", None);
    if let Some(name) = &cli_collection {
        crate::collections::validate_name(name).map_err(|e| format!("--collection: {e}"))?;
    }
    for (i, shard) in file.shards.iter().enumerate() {
        if let Some(name) = &shard.collection {
            crate::collections::validate_name(name)
                .map_err(|e| format!("shards[{i}].collection: {e}"))?;
        }
    }
    let shards: Vec<ShardConfig> = if cli_index.is_some() || cli_demo.is_some() {
        if cli_index.is_some() && cli_demo.is_some() {
            return Err("--index and --demo-vectors are mutually exclusive".to_string());
        }
        let listen = node_listen_default
            .parse::<SocketAddr>()
            .map_err(|e| format!("invalid node listen address: {e}"))?;
        let demo = cli_demo
            .map(|s| {
                s.parse::<usize>()
                    .map(|vectors| DemoConfig {
                        vectors,
                        dim,
                        bit_width,
                    })
                    .map_err(|e| format!("invalid demo vector count: {e}"))
            })
            .transpose()?;
        vec![ShardConfig {
            listen,
            vector_backend: vector_backend.clone(),
            wal: wal_default && demo.is_none(),
            wal_buckets: wal_buckets_default,
            vocab: vocab_default && demo.is_none(),
            index_path: cli_index.map(PathBuf::from),
            demo,
            slot_offset: cli_offset,
            analysis_addr: None,
            collection: cli_collection.clone().unwrap_or_default(),
            shard_id: opt(args, "shard-id", "PIPESTREAM_SEARCH_SHARD_ID", None),
            hash_range: parse_hash_range(
                opt(args, "hash-lo", "PIPESTREAM_SEARCH_HASH_LO", None)
                    .map(|v| {
                        v.parse::<u64>()
                            .map_err(|e| format!("invalid --hash-lo: {e}"))
                    })
                    .transpose()?,
                opt(args, "hash-hi", "PIPESTREAM_SEARCH_HASH_HI", None)
                    .map(|v| {
                        v.parse::<u64>()
                            .map_err(|e| format!("invalid --hash-hi: {e}"))
                    })
                    .transpose()?,
                "--hash-lo/--hash-hi",
            )?,
        }]
    } else {
        file.shards
            .iter()
            .enumerate()
            .map(|(i, shard)| {
                let listen = shard
                    .listen
                    .clone()
                    .unwrap_or_else(|| node_listen_default.clone())
                    .parse::<SocketAddr>()
                    .map_err(|e| format!("shards[{i}]: invalid listen address: {e}"))?;
                let demo = shard.demo_vectors.map(|vectors| DemoConfig {
                    vectors,
                    dim,
                    bit_width,
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
                        .unwrap_or_else(|| vector_backend.clone()),
                    wal: shard.wal.unwrap_or(wal_default) && demo.is_none(),
                    wal_buckets: parse_buckets(
                        shard.wal_buckets.unwrap_or(wal_buckets_default),
                        &format!("shards[{i}]"),
                    )?,
                    vocab: shard.vocab.unwrap_or(vocab_default) && demo.is_none(),
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
            })
            .collect::<Result<_, String>>()?
    };
    Ok(ShardSet {
        shards,
        vocab_window_docs,
        vocab_top_k,
    })
}
