//! `parse`: every option in precedence order, then the cross-option checks.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::chunked::DEFAULT_CHUNK_BLOCKS;
use crate::MAX_MESSAGE_BYTES;

use super::args::{
    control_u64, flag_present, normalize_addrs, normalize_analysis_backend, opt, parse_env_bool,
};
use super::columns::{parse_column_fields, parse_phrase_fields, ColumnFields, PhraseFields};
use super::file::{read_file_config, FileConfig};
use super::membership::parse_membership;
use super::raft::parse_raft;
use super::shard_map::{load_placement_tree, load_shard_map};
use super::shards::{parse_clustered_turbovec, parse_shards, ShardSet};
use super::tls::parse_tls;
use super::turbo::{parse_turbo, TurboOptions};
use super::{CollectionConfig, Config, Role};

/// Parse configuration from process args (excluding `argv[0]`).
pub fn parse(args: &[String]) -> Result<Config, String> {
    // The config file sits at the bottom of the precedence stack.
    let file: FileConfig = read_file_config(args)?;

    let role = match opt(args, "role", "PIPESTREAM_SEARCH_ROLE", file.role.as_deref())
        .unwrap_or_else(|| "node".to_string())
        .as_str()
    {
        "node" => Role::Node,
        "coordinator" => Role::Coordinator,
        "both" => Role::Both,
        other => return Err(format!("unknown role {other:?} (node|coordinator|both)")),
    };

    let coord_listen = opt(
        args,
        "coord-listen",
        "PIPESTREAM_SEARCH_COORD_LISTEN",
        file.coord_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50050".to_string())
    .parse::<SocketAddr>()
    .map_err(|e| format!("invalid coordinator listen address: {e}"))?;

    let metrics_listen = opt(
        args,
        "metrics-listen",
        "PIPESTREAM_SEARCH_METRICS_LISTEN",
        file.metrics_listen.as_deref(),
    )
    .map(|a| {
        a.parse::<SocketAddr>()
            .map_err(|e| format!("invalid metrics listen address: {e}"))
    })
    .transpose()?;

    // Coordinator fan-out list. A shard map (--shard-map) REPLACES
    // --nodes: it carries the same addresses plus topology metadata.
    let shard_map_path = opt(
        args,
        "shard-map",
        "PIPESTREAM_SEARCH_SHARD_MAP",
        file.shard_map.as_deref(),
    )
    .map(PathBuf::from);
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let shard_map_reload_ms = opt(
        args,
        "shard-map-reload-ms",
        "PIPESTREAM_SEARCH_SHARD_MAP_RELOAD_MS",
        file.shard_map_reload_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|value| {
        value
            .parse::<u64>()
            .map_err(|e| format!("invalid --shard-map-reload-ms: {e}"))
    })
    .transpose()?
    .unwrap_or_else(|| u64::from(shard_map_path.is_some()) * 1_000);
    if shard_map_reload_ms > 0 && shard_map_path.is_none() {
        return Err("--shard-map-reload-ms requires --shard-map".to_string());
    }
    let nodes_given =
        opt(args, "nodes", "PIPESTREAM_SEARCH_NODES", None).is_some() || file.nodes.is_some();
    if shard_map.is_some() && nodes_given {
        return Err("--shard-map replaces --nodes; pass exactly one".to_string());
    }
    let node_addrs = match &shard_map {
        Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
        None => match opt(args, "nodes", "PIPESTREAM_SEARCH_NODES", None) {
            Some(s) => normalize_addrs(
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
            None => normalize_addrs(file.nodes.clone().unwrap_or_default()),
        },
    };

    let clustered_turbovec = parse_clustered_turbovec(args, &file)?;

    let dim = opt(
        args,
        "dim",
        "PIPESTREAM_SEARCH_DIM",
        file.dim.map(|d| d.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "128".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid dim: {e}"))?;
    let bit_width = opt(
        args,
        "bit-width",
        "PIPESTREAM_SEARCH_BIT_WIDTH",
        file.bit_width.map(|b| b.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "4".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid bit width: {e}"))?;
    let vector_backend = opt(
        args,
        "vector-backend",
        "PIPESTREAM_SEARCH_VECTOR_BACKEND",
        file.vector_backend.as_deref(),
    )
    .unwrap_or_else(|| crate::vector::EMBEDDED_TURBOVEC.to_string());

    let ShardSet {
        mut shards,
        vocab_window_docs,
        vocab_top_k,
    } = parse_shards(args, &file, dim, bit_width, &vector_backend)?;

    let chunk_blocks = opt(
        args,
        "chunk-blocks",
        "PIPESTREAM_SEARCH_CHUNK_BLOCKS",
        file.chunk_blocks.map(|c| c.to_string()).as_deref(),
    )
    .unwrap_or_else(|| DEFAULT_CHUNK_BLOCKS.to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid chunk blocks: {e}"))?;

    let share_floors = match opt(
        args,
        "floor-sharing",
        "PIPESTREAM_SEARCH_FLOOR_SHARING",
        None,
    ) {
        Some(s) => parse_env_bool(&s),
        None => file.floor_sharing.unwrap_or(true),
    };
    let block_max = match opt(args, "block-max", "PIPESTREAM_SEARCH_BLOCK_MAX", None) {
        Some(s) => parse_env_bool(&s),
        None => file.block_max.unwrap_or(true),
    };
    let segment_pruning = match opt(
        args,
        "segment-pruning",
        "PIPESTREAM_SEARCH_SEGMENT_PRUNING",
        None,
    ) {
        Some(s) => parse_env_bool(&s),
        None => file.segment_pruning.unwrap_or(true),
    };
    let shard_pruning = match opt(
        args,
        "shard-pruning",
        "PIPESTREAM_SEARCH_SHARD_PRUNING",
        None,
    ) {
        Some(s) => parse_env_bool(&s),
        None => file.shard_pruning.unwrap_or(true),
    };
    let placement_column = opt(
        args,
        "placement-column",
        "PIPESTREAM_SEARCH_PLACEMENT_COLUMN",
        file.placement_column.as_deref(),
    )
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty());
    let placement_leaf = opt(
        args,
        "placement-leaf",
        "PIPESTREAM_SEARCH_PLACEMENT_LEAF",
        file.placement_leaf.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.trim()
            .parse::<i64>()
            .map_err(|e| format!("invalid placement leaf {s:?}: {e}"))
            .and_then(|code| {
                if code < 0 {
                    Err(format!(
                        "placement leaf {code} is negative; codes are non-negative path codes"
                    ))
                } else {
                    Ok(code)
                }
            })
    })
    .transpose()?;
    if placement_leaf.is_some() && placement_column.is_none() {
        return Err(
            "--placement-leaf needs --placement-column, the column that holds the code".to_string(),
        );
    }
    let placement_tree = match opt(
        args,
        "placement-tree",
        "PIPESTREAM_SEARCH_PLACEMENT_TREE",
        file.placement_tree.as_deref(),
    )
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
    {
        Some(path) => {
            let (Some(column), Some(code)) = (placement_column.as_deref(), placement_leaf) else {
                return Err(
                    "--placement-tree needs --placement-leaf (and --placement-column): the \
                     tree is checked on the leaf this shard pins"
                        .to_string(),
                );
            };
            let tree = load_placement_tree(std::path::Path::new(&path))?;
            let pinned = crate::placement::PinnedLeaf::pin(&tree, column, code)
                .map_err(|e| format!("placement tree {path}: {e}"))?;
            Some(std::sync::Arc::new(pinned))
        }
        None => None,
    };
    // The derived-column declaration (docs/derived-columns.md): a file
    // (the coordinator's shard map or a bare [[derived]] table) or the
    // config file's own [[derived]] table, never both.
    let derived = {
        let path = opt(
            args,
            "derived-columns",
            "PIPESTREAM_SEARCH_DERIVED_COLUMNS",
            file.derived_columns.as_deref(),
        )
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
        let spec = match (path, file.derived.is_empty()) {
            (Some(_), false) => {
                return Err(
                    "--derived-columns names a file and the config file has its own [[derived]] \
                     table; declare the columns in one place"
                        .to_string(),
                );
            }
            (Some(path), true) => Some(
                crate::derived::load_declaration(std::path::Path::new(&path))
                    .map_err(|e| format!("derived columns {path}: {e}"))?,
            ),
            (None, false) => Some(crate::derived::spec_from_config(&file.derived)?),
            (None, true) => None,
        };
        spec.map(|spec| {
            crate::derived::Declaration::compile(&spec)
                .map(std::sync::Arc::new)
                .map_err(|e| format!("derived columns: {e}"))
        })
        .transpose()?
    };
    let allow_missing_bm25 = flag_present(args, "allow-missing-bm25")
        || match opt(
            args,
            "allow-missing-bm25",
            "PIPESTREAM_SEARCH_ALLOW_MISSING_BM25",
            None,
        ) {
            Some(s) => parse_env_bool(&s),
            None => file.allow_missing_bm25.unwrap_or(false),
        };
    let coalesce = match opt(args, "coalesce", "PIPESTREAM_SEARCH_COALESCE", None) {
        Some(s) => parse_env_bool(&s),
        None => file.coalesce.unwrap_or(true),
    };
    let scan_parallel = opt(
        args,
        "scan-parallel",
        "PIPESTREAM_SEARCH_SCAN_PARALLEL",
        file.scan_parallel.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid scan parallel: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let rerank_parallel = opt(
        args,
        "rerank-parallel",
        "PIPESTREAM_SEARCH_RERANK_PARALLEL",
        file.rerank_parallel.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid rerank parallel: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    if rerank_parallel > crate::node::MAX_RERANK_PARALLEL {
        return Err(format!(
            "rerank parallel must be <= {}, got {rerank_parallel}",
            crate::node::MAX_RERANK_PARALLEL
        ));
    }
    let floor_delta = opt(
        args,
        "floor-delta",
        "PIPESTREAM_SEARCH_FLOOR_DELTA",
        file.floor_delta.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<f32>()
            .map_err(|e| format!("invalid floor delta: {e}"))
            .and_then(|v| {
                if v.is_finite() && v >= 0.0 {
                    Ok(v)
                } else {
                    Err(format!("floor delta must be finite and >= 0, got {v}"))
                }
            })
    })
    .transpose()?
    .unwrap_or(0.0);
    let floor_warmup_chunks = opt(
        args,
        "floor-warmup-chunks",
        "PIPESTREAM_SEARCH_FLOOR_WARMUP_CHUNKS",
        file.floor_warmup_chunks.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid floor-warmup-chunks: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let floor_min_interval_ms = opt(
        args,
        "floor-min-interval-ms",
        "PIPESTREAM_SEARCH_FLOOR_MIN_INTERVAL_MS",
        file.floor_min_interval_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid floor-min-interval-ms: {e}"))
    })
    .transpose()?
    .unwrap_or(0);
    let parse_ms = |key: &str, env: &str, file_val: Option<u64>| -> Result<u64, String> {
        opt(args, key, env, file_val.map(|v| v.to_string()).as_deref())
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|e| format!("invalid --{key}: {e}"))
            })
            .transpose()
            .map(|v| v.unwrap_or(0))
    };
    let shard_deadline_ms = parse_ms(
        "shard-deadline-ms",
        "PIPESTREAM_SEARCH_SHARD_DEADLINE_MS",
        file.shard_deadline_ms,
    )?;
    let hedge_delay_ms = parse_ms(
        "hedge-delay-ms",
        "PIPESTREAM_SEARCH_HEDGE_DELAY_MS",
        file.hedge_delay_ms,
    )?;
    let replica_addrs: Vec<Option<String>> = match &shard_map {
        Some(map) => map
            .shards
            .iter()
            .map(|s| {
                s.replica
                    .clone()
                    .map(|a| normalize_addrs(vec![a]).remove(0))
            })
            .collect(),
        None => Vec::new(),
    };
    let replica_sync_ms = opt(
        args,
        "replica-sync-ms",
        "PIPESTREAM_SEARCH_REPLICA_SYNC_MS",
        file.replica_sync_ms.map(|v| v.to_string()).as_deref(),
    )
    .map(|value| {
        value
            .parse::<u64>()
            .map_err(|e| format!("invalid --replica-sync-ms: {e}"))
    })
    .transpose()?
    .unwrap_or_else(|| u64::from(replica_addrs.iter().any(Option::is_some)) * 1_000);
    let replica_state_path = opt(
        args,
        "replica-state",
        "PIPESTREAM_SEARCH_REPLICA_STATE",
        file.replica_state.as_deref(),
    )
    .map(PathBuf::from)
    .or_else(|| {
        shard_map_path.as_ref().map(|path| {
            let mut name = path.as_os_str().to_owned();
            name.push(".replica-sync.toml");
            PathBuf::from(name)
        })
    });
    if replica_sync_ms > 0 && replica_state_path.is_none() {
        return Err("automatic replica sync requires --replica-state or --shard-map".to_string());
    }
    let control_state_path = opt(
        args,
        "control-state",
        "PIPESTREAM_SEARCH_CONTROL_STATE",
        file.control_state.as_deref(),
    )
    .map(PathBuf::from);
    if control_state_path.is_some() && shard_map.is_none() {
        return Err("--control-state requires a generation-stamped --shard-map".to_string());
    }
    let parse_control_u64 = |key: &str, env: &str, file_value: Option<u64>, default: u64| {
        control_u64(args, key, env, file_value, default)
    };
    let control_reconcile_ms = parse_control_u64(
        "control-reconcile-ms",
        "PIPESTREAM_SEARCH_CONTROL_RECONCILE_MS",
        file.control_reconcile_ms,
        u64::from(control_state_path.is_some()) * 1_000,
    )?;
    let control_lease_ms = parse_control_u64(
        "control-lease-ms",
        "PIPESTREAM_SEARCH_CONTROL_LEASE_MS",
        file.control_lease_ms,
        15_000,
    )?;
    let control_replication_factor = usize::try_from(parse_control_u64(
        "control-replication-factor",
        "PIPESTREAM_SEARCH_CONTROL_REPLICATION_FACTOR",
        file.control_replication_factor.map(|value| value as u64),
        2,
    )?)
    .map_err(|_| "--control-replication-factor does not fit usize".to_string())?;
    let control_split_rows = parse_control_u64(
        "control-split-rows",
        "PIPESTREAM_SEARCH_CONTROL_SPLIT_ROWS",
        file.control_split_rows,
        25_000_000,
    )?;
    let control_merge_rows = parse_control_u64(
        "control-merge-rows",
        "PIPESTREAM_SEARCH_CONTROL_MERGE_ROWS",
        file.control_merge_rows,
        2_000_000,
    )?;
    let control_compact_segments = u32::try_from(parse_control_u64(
        "control-compact-segments",
        "PIPESTREAM_SEARCH_CONTROL_COMPACT_SEGMENTS",
        file.control_compact_segments.map(u64::from),
        8,
    )?)
    .map_err(|_| "--control-compact-segments exceeds u32".to_string())?;
    let control_compact_tombstone_ppm = u32::try_from(parse_control_u64(
        "control-compact-tombstone-ppm",
        "PIPESTREAM_SEARCH_CONTROL_COMPACT_TOMBSTONE_PPM",
        file.control_compact_tombstone_ppm.map(u64::from),
        100_000,
    )?)
    .map_err(|_| "--control-compact-tombstone-ppm exceeds u32".to_string())?;
    if control_state_path.is_some()
        && (!matches!(role, Role::Coordinator | Role::Both)
            || control_replication_factor == 0
            || control_lease_ms < 1_000
            || control_split_rows == 0
            || control_merge_rows == 0
            || control_compact_segments == 0
            || control_compact_tombstone_ppm == 0
            || control_compact_tombstone_ppm > 1_000_000)
    {
        return Err("control plane requires coordinator/both role, positive thresholds and replication factor, lease >= 1000ms, and tombstone ppm <= 1000000".to_string());
    }

    let membership = parse_membership(args, &file, role)?;

    let max_message_bytes = opt(
        args,
        "max-message-mib",
        "PIPESTREAM_SEARCH_MAX_MESSAGE_MIB",
        file.max_message_mib.map(|m| m.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map(|mib| mib * 1024 * 1024)
            .map_err(|e| format!("invalid max message MiB: {e}"))
    })
    .transpose()?
    .unwrap_or(MAX_MESSAGE_BYTES);

    let demo_query = flag_present(args, "demo-query")
        || std::env::var("PIPESTREAM_SEARCH_DEMO_QUERY")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.demo_query.unwrap_or(false);
    let query_dim = opt(
        args,
        "query-dim",
        "PIPESTREAM_SEARCH_QUERY_DIM",
        file.query_dim.map(|d| d.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<usize>()
            .map_err(|e| format!("invalid query dim: {e}"))
    })
    .transpose()?
    .unwrap_or(dim);

    if matches!(role, Role::Node | Role::Both) && shards.is_empty() {
        return Err(
            "node/both role requires at least one shard (--index/--demo-vectors or [[shards]])"
                .to_string(),
        );
    }
    if matches!(role, Role::Coordinator | Role::Both)
        && node_addrs.is_empty()
        && file.collections.is_empty()
    {
        return Err(
            "coordinator role requires --nodes or --shard-map (or `nodes` or `collections` in \
             the config file)"
                .to_string(),
        );
    }
    if role == Role::Node && clustered_turbovec.is_some() {
        return Err(
            "clustered TurboVec is a product-coordinator backend and cannot be configured on a node-only process"
                .to_string(),
        );
    }
    if demo_query && role == Role::Node {
        return Err("--demo-query requires the coordinator or both role".to_string());
    }

    let save_on_shutdown = match opt(
        args,
        "save-on-shutdown",
        "PIPESTREAM_SEARCH_SAVE_ON_SHUTDOWN",
        None,
    ) {
        Some(s) => parse_env_bool(&s),
        None => file.save_on_shutdown.unwrap_or(true),
    };

    let stream_search = flag_present(args, "stream-search")
        || std::env::var("PIPESTREAM_SEARCH_STREAM_SEARCH")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.stream_search.unwrap_or(false);

    let parse_window_us =
        |key: &str, env: &str, from_file: Option<u32>| -> Result<Option<u32>, String> {
            let value = match opt(args, key, env, None) {
                Some(raw) => Some(
                    raw.parse::<u32>()
                        .map_err(|e| format!("invalid --{key}: {e}"))?,
                ),
                None => from_file,
            };
            if let Some(us) = value {
                if !(1..=crate::dense_policy::MAX_FINISH_SETTLE_US).contains(&us) {
                    return Err(format!(
                        "--{key} {us} is outside 1..={}",
                        crate::dense_policy::MAX_FINISH_SETTLE_US
                    ));
                }
            }
            Ok(value)
        };
    let stream_finish_settle_us = parse_window_us(
        "stream-finish-settle-us",
        "PIPESTREAM_SEARCH_STREAM_FINISH_SETTLE_US",
        file.stream_finish_settle_us,
    )?;
    let stream_finish_deadline_us = parse_window_us(
        "stream-finish-deadline-us",
        "PIPESTREAM_SEARCH_STREAM_FINISH_DEADLINE_US",
        file.stream_finish_deadline_us,
    )?;
    let stream_poll_delay_us = match opt(
        args,
        "stream-poll-delay-us",
        "PIPESTREAM_SEARCH_STREAM_POLL_DELAY_US",
        None,
    ) {
        Some(raw) => Some(
            raw.parse::<u64>()
                .map_err(|e| format!("invalid --stream-poll-delay-us: {e}"))?,
        ),
        None => file.stream_poll_delay_us,
    };

    let relay = flag_present(args, "relay")
        || std::env::var("PIPESTREAM_SEARCH_RELAY")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.relay.unwrap_or(false);
    if relay {
        if role != Role::Coordinator {
            return Err(
                "--relay serves the node-facing surface over a coordinator's shard set and \
                 takes --role=coordinator only"
                    .to_string(),
            );
        }
        if !file.collections.is_empty() {
            return Err(
                "--relay serves one unnamed collection on a dedicated endpoint; named \
                 [[collections]] are not multiplexed through a relay"
                    .to_string(),
            );
        }
    }

    let bm25_stream = if flag_present(args, "bm25-stream") {
        true
    } else {
        opt(
            args,
            "bm25-stream",
            "PIPESTREAM_SEARCH_BM25_STREAM",
            file.bm25_stream.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(true)
    };

    let max_k = opt(
        args,
        "max-k",
        "PIPESTREAM_SEARCH_MAX_K",
        file.max_k.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid max k: {e}"))
            .and_then(|v| {
                if v == 0 {
                    Err("max k must be at least 1 (0 would refuse every query)".to_string())
                } else {
                    Ok(v)
                }
            })
    })
    .transpose()?
    .unwrap_or(crate::coordinator::DEFAULT_MAX_K);
    let signal_batch = opt(
        args,
        "signal-batch",
        "PIPESTREAM_SEARCH_SIGNAL_BATCH",
        file.signal_batch.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u32>()
            .map_err(|e| format!("invalid signal batch: {e}"))
            .and_then(|v| {
                if v == 0 {
                    Err("signal batch must be at least 1 (ids per rescore call)".to_string())
                } else {
                    Ok(v)
                }
            })
    })
    .transpose()?
    .unwrap_or(crate::coordinator::DEFAULT_SIGNAL_BATCH);
    let max_rerank_bytes = opt(
        args,
        "max-rerank-mib",
        "PIPESTREAM_SEARCH_MAX_RERANK_MIB",
        file.max_rerank_mib.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<u64>()
            .map_err(|e| format!("invalid max rerank MiB: {e}"))
            .and_then(|mib| {
                if mib == 0 {
                    return Err("max rerank MiB must be positive".to_string());
                }
                mib.checked_mul(1024 * 1024)
                    .ok_or_else(|| "max rerank MiB overflows bytes".to_string())
            })
    })
    .transpose()?
    .unwrap_or(crate::coordinator::DEFAULT_MAX_RERANK_BYTES);
    let dense_quality_profile = opt(
        args,
        "dense-quality-profile",
        "PIPESTREAM_SEARCH_DENSE_QUALITY_PROFILE",
        file.dense_quality_profile.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);
    let synonyms = opt(
        args,
        "synonyms",
        "PIPESTREAM_SEARCH_SYNONYMS",
        file.synonyms.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);
    let dense_execution_policy = opt(
        args,
        "dense-execution-policy",
        "PIPESTREAM_SEARCH_DENSE_EXECUTION_POLICY",
        file.dense_execution_policy.as_deref(),
    )
    .filter(|path| !path.trim().is_empty())
    .map(PathBuf::from);

    let analysis_addr = opt(
        args,
        "analysis-addr",
        "PIPESTREAM_SEARCH_ANALYSIS_ADDR",
        file.analysis_addr.as_deref(),
    )
    .map(normalize_analysis_backend);
    // A single-shard CLI setup shares the analysis backend with its shard.
    if analysis_addr.is_some() && shards.len() == 1 && shards[0].analysis_addr.is_none() {
        shards[0].analysis_addr.clone_from(&analysis_addr);
    }
    let bm25_k1 = opt(
        args,
        "bm25-k1",
        "PIPESTREAM_SEARCH_BM25_K1",
        file.bm25_k1.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| {
        s.parse::<f32>()
            .map_err(|e| format!("invalid bm25 k1: {e}"))
    })
    .transpose()?
    .unwrap_or(1.2);
    let bm25_b = opt(
        args,
        "bm25-b",
        "PIPESTREAM_SEARCH_BM25_B",
        file.bm25_b.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| s.parse::<f32>().map_err(|e| format!("invalid bm25 b: {e}")))
    .transpose()?
    .unwrap_or(0.75);

    let ColumnFields {
        bm25_fields,
        facet_fields,
        numeric_fields,
        map_facet_fields,
        map_numeric_fields,
        map_integer_fields,
        map_unsigned_integer_fields,
        integer_fields,
        unsigned_integer_fields,
        geo_fields,
    } = parse_column_fields(args, &file, &placement_column)?;

    let PhraseFields {
        phrase_glossary,
        phrase_field,
        entity_map_field,
        phrase_ignore_case,
        phrase_ner,
        position_fields,
        bigram_fields,
        sentence_fields,
    } = parse_phrase_fields(args, &file, &bm25_fields, &map_facet_fields)?;

    // Named collections (docs/collections.md): each carries its own
    // dataset settings; the top-level dataset knobs must then be absent,
    // because there is no unnamed dataset to apply them to.
    let mut collections: Vec<CollectionConfig> = Vec::with_capacity(file.collections.len());
    if !file.collections.is_empty() {
        if !node_addrs.is_empty() {
            return Err(
                "`collections` replaces --nodes / --shard-map: put each collection's nodes or \
                 shard_map on the collection"
                    .to_string(),
            );
        }
        if control_state_path.is_some() {
            return Err(
                "`collections` replaces --control-state: put each collection's control_state on \
                 the collection"
                    .to_string(),
            );
        }
        if replica_state_path.is_some() {
            return Err(
                "`collections` replaces --replica-state: put each collection's replica_state on \
                 the collection"
                    .to_string(),
            );
        }
        if clustered_turbovec.is_some() {
            return Err(
                "clustered TurboVec serves one dataset and is not yet configurable per \
                 collection; drop `collections` or the clustered backend"
                    .to_string(),
            );
        }
        let mut owners: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (i, c) in file.collections.iter().enumerate() {
            let at = format!("collections[{i}]");
            crate::collections::validate_name(&c.name).map_err(|e| format!("{at}: {e}"))?;
            if collections.iter().any(|known| known.name == c.name) {
                return Err(format!("{at}: collection {:?} is declared twice", c.name));
            }
            let shard_map_path = c.shard_map.as_ref().map(PathBuf::from);
            if shard_map_path.is_some() && c.nodes.is_some() {
                return Err(format!("{at}: shard_map replaces nodes; give only one"));
            }
            let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
            let node_addrs = match &shard_map {
                Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
                None => normalize_addrs(c.nodes.clone().unwrap_or_default()),
            };
            if node_addrs.is_empty() {
                return Err(format!("{at}: a collection needs nodes or a shard_map"));
            }
            for addr in &node_addrs {
                if let Some(other) = owners.insert(addr.clone(), c.name.clone()) {
                    return Err(format!(
                        "{at}: node {addr} is already listed under collection {other:?}; a shard \
                         belongs to only one collection"
                    ));
                }
            }
            let replica_addrs: Vec<Option<String>> = match &shard_map {
                Some(map) => map
                    .shards
                    .iter()
                    .map(|s| {
                        s.replica
                            .clone()
                            .map(|r| normalize_addrs(vec![r]).remove(0))
                    })
                    .collect(),
                None => Vec::new(),
            };
            let control_state_path = c.control_state.as_ref().map(PathBuf::from);
            if control_state_path.is_some() && shard_map.is_none() {
                return Err(format!(
                    "{at}: control_state requires a generation-written shard_map"
                ));
            }
            let replica_state_path = c.replica_state.as_ref().map(PathBuf::from);
            if replica_sync_ms > 0
                && replica_addrs.iter().any(Option::is_some)
                && replica_state_path.is_none()
            {
                return Err(format!(
                    "{at}: automatic replica sync needs replica_state on the collection"
                ));
            }
            if shard_map_reload_ms > 0 && shard_map_path.is_none() {
                return Err(format!(
                    "{at}: --shard-map-reload-ms needs a shard_map on every collection"
                ));
            }
            collections.push(CollectionConfig {
                name: c.name.clone(),
                node_addrs,
                replica_addrs,
                shard_map,
                shard_map_path,
                analysis_addr: c
                    .analysis_addr
                    .clone()
                    .map(normalize_analysis_backend)
                    .or_else(|| analysis_addr.clone()),
                bm25_k1: c.bm25_k1.unwrap_or(bm25_k1),
                bm25_b: c.bm25_b.unwrap_or(bm25_b),
                dense_quality_profile: c
                    .dense_quality_profile
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| dense_quality_profile.clone()),
                synonyms: c
                    .synonyms
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| synonyms.clone()),
                dense_execution_policy: c
                    .dense_execution_policy
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| dense_execution_policy.clone()),
                replica_state_path,
                control_state_path,
            });
        }
    }
    let (tls, client_tls) = parse_tls(args, &file)?;
    let path_opt = |key: &str, env: &str, file_value: Option<&str>| {
        opt(args, key, env, file_value).map(PathBuf::from)
    };
    let allow_plaintext = flag_present(args, "allow-plaintext")
        || std::env::var("PIPESTREAM_SEARCH_ALLOW_PLAINTEXT")
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.allow_plaintext.unwrap_or(false);
    if tls.is_some() && allow_plaintext {
        return Err(
            "--allow-plaintext contradicts --tls-cert: once TLS is set, plaintext is refused"
                .to_string(),
        );
    }
    if tls.is_none() && !allow_plaintext {
        let mut listeners: Vec<SocketAddr> = shards.iter().map(|s| s.listen).collect();
        if matches!(role, Role::Coordinator | Role::Both) {
            listeners.push(coord_listen);
        }
        if let Some(addr) = listeners
            .iter()
            .find(|addr| !crate::security::is_loopback(addr))
        {
            return Err(format!(
                "listener {addr} is not loopback and no TLS is configured; pass --tls-cert and \
                 --tls-key, or --allow-plaintext to serve plaintext gRPC there on purpose"
            ));
        }
    }
    if matches!(role, Role::Node | Role::Both)
        && tls.as_ref().is_some_and(|t| t.client_ca_pem.is_none())
    {
        return Err(
            "node listeners run mTLS: --tls-client-ca (the cluster CA) is required with --tls-cert"
                .to_string(),
        );
    }
    if matches!(role, Role::Coordinator | Role::Both) && tls.is_some() && client_tls.is_none() {
        return Err(
            "a TLS coordinator needs --tls-ca (and --tls-client-cert / --tls-client-key, its \
             membership) to reach its nodes"
                .to_string(),
        );
    }
    let principals = path_opt(
        "bearer-tokens",
        "PIPESTREAM_SEARCH_BEARER_TOKENS",
        file.bearer_tokens.as_deref(),
    )
    .map(|path| crate::security::Principals::load(&path).map(std::sync::Arc::new))
    .transpose()?;
    let udp_hmac_key = path_opt(
        "udp-hmac-key",
        "PIPESTREAM_SEARCH_UDP_HMAC_KEY",
        file.udp_hmac_key.as_deref(),
    )
    .map(|path| crate::security::UdpKey::load(&path))
    .transpose()?;
    let vector_mmap =
        match opt(args, "vector-mmap", "PIPESTREAM_SEARCH_VECTOR_MMAP", None).as_deref() {
            None => file.vector_mmap.unwrap_or(true),
            Some("true") => true,
            Some("false") => false,
            Some(other) => {
                return Err(format!(
                    "--vector-mmap={other:?} is not a boolean; use true (the default) or false"
                ))
            }
        };
    let layout = match opt(
        args,
        "layout",
        "PIPESTREAM_SEARCH_LAYOUT",
        file.layout.as_deref(),
    )
    .as_deref()
    {
        None | Some("segments") => crate::node::Layout::Segments,
        Some("single-image") => crate::node::Layout::SingleImage,
        Some(other) => {
            return Err(format!(
                "--layout={other:?} is not a layout; use segments (the default) or single-image"
            ))
        }
    };
    let seal_tail_docs = opt(
        args,
        "seal-tail-docs",
        "PIPESTREAM_SEARCH_SEAL_TAIL_DOCS",
        file.seal_tail_docs.map(|v| v.to_string()).as_deref(),
    )
    .map(|v| {
        v.parse::<u32>()
            .map_err(|e| format!("invalid --seal-tail-docs: {e}"))
    })
    .transpose()?
    .unwrap_or(500_000);
    let default_collection = file.default_collection.clone();
    if let Some(name) = &default_collection {
        if !collections.iter().any(|c| &c.name == name) {
            return Err(format!(
                "default_collection {name:?} is not a declared collection ({:?})",
                collections
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
            ));
        }
    }
    if !collections.is_empty() && matches!(role, Role::Node | Role::Both) {
        for (i, shard) in shards.iter().enumerate() {
            if !collections.iter().any(|c| c.name == shard.collection) {
                return Err(format!(
                    "shards[{i}] serves collection {:?}, which this process's `collections` \
                     does not declare ({:?})",
                    shard.collection,
                    collections
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                ));
            }
        }
    }

    let raft = parse_raft(args, &file, relay)?;

    let TurboOptions {
        turbo_nlp_columns,
        turbo_one_pass_query,
        turbo_rerank_endpoint,
        turbo_rerank_model,
        turbo_matryoshka_dims,
        turbo_matryoshka_active_dim,
        turbo_vocab_endpoint,
        turbo_model_sha256,
        turbo_bearer_token,
    } = parse_turbo(args, &file)?;

    Ok(Config {
        role,
        coord_listen,
        raft,
        metrics_listen,
        node_addrs,
        clustered_turbovec,
        shards,
        chunk_blocks,
        share_floors,
        block_max,
        segment_pruning,
        shard_pruning,
        placement_column,
        placement_leaf,
        placement_tree,
        derived,
        allow_missing_bm25,
        coalesce,
        scan_parallel,
        rerank_parallel,
        floor_delta,
        floor_warmup_chunks,
        floor_min_interval_ms,
        shard_deadline_ms,
        hedge_delay_ms,
        replica_addrs,
        max_message_bytes,
        demo_query,
        stream_search,
        stream_finish_settle_us,
        stream_finish_deadline_us,
        stream_poll_delay_us,
        relay,
        bm25_stream,
        max_k,
        signal_batch,
        max_rerank_bytes,
        dense_quality_profile,
        synonyms,
        dense_execution_policy,
        query_dim,
        bit_width,
        save_on_shutdown,
        analysis_addr,
        bm25_k1,
        bm25_b,
        bm25_fields,
        facet_fields,
        numeric_fields,
        map_facet_fields,
        phrase_glossary,
        phrase_field,
        entity_map_field,
        phrase_ignore_case,
        phrase_ner,
        position_fields,
        bigram_fields,
        sentence_fields,
        map_numeric_fields,
        map_integer_fields,
        map_unsigned_integer_fields,
        integer_fields,
        unsigned_integer_fields,
        geo_fields,
        shard_map,
        shard_map_path,
        shard_map_reload_ms,
        replica_sync_ms,
        replica_state_path,
        control_state_path,
        control_reconcile_ms,
        control_lease_ms,
        control_replication_factor,
        control_split_rows,
        control_merge_rows,
        control_compact_segments,
        control_compact_tombstone_ppm,
        membership,
        collections,
        default_collection,
        tls,
        client_tls,
        allow_plaintext,
        principals,
        udp_hmac_key,
        layout,
        seal_tail_docs,
        vector_mmap,
        vocab_window_docs,
        vocab_top_k,
        turbo_nlp_columns,
        turbo_one_pass_query,
        turbo_rerank_endpoint,
        turbo_rerank_model,
        turbo_matryoshka_dims,
        turbo_matryoshka_active_dim,
        turbo_vocab_endpoint,
        turbo_model_sha256,
        turbo_bearer_token,
    })
}
