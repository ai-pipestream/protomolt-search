use super::*;

pub fn parse(args: &[String]) -> Result<Config, String> {
    // The config file sits at the bottom of the precedence stack.
    let file: FileConfig = read_file_config(args)?;

    let role = match opt(args, "role", "TURBOVEC_ROLE", file.role.as_deref())
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
        "TURBOVEC_COORD_LISTEN",
        file.coord_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50050".to_string())
    .parse::<SocketAddr>()
    .map_err(|e| format!("invalid coordinator listen address: {e}"))?;

    let metrics_listen = opt(
        args,
        "metrics-listen",
        "TURBOVEC_METRICS_LISTEN",
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
        "TURBOVEC_SHARD_MAP",
        file.shard_map.as_deref(),
    )
    .map(PathBuf::from);
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let shard_map_reload_ms = opt(
        args,
        "shard-map-reload-ms",
        "TURBOVEC_SHARD_MAP_RELOAD_MS",
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
    let nodes_given = opt(args, "nodes", "TURBOVEC_NODES", None).is_some() || file.nodes.is_some();
    if shard_map.is_some() && nodes_given {
        return Err("--shard-map replaces --nodes; pass exactly one".to_string());
    }
    let node_addrs = match &shard_map {
        Some(map) => normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
        None => match opt(args, "nodes", "TURBOVEC_NODES", None) {
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

    let clustered_file = file.clustered_turbovec.as_ref();
    let clustered_endpoint = opt(
        args,
        "turbovec-coordinator",
        "TURBOVEC_COORDINATOR",
        clustered_file.and_then(|config| config.coordinator.as_deref()),
    );
    let clustered_nodes = opt(
        args,
        "turbovec-cluster-nodes",
        "TURBOVEC_CLUSTER_NODES",
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
        "TURBOVEC_CLUSTER_STATE",
        clustered_file.and_then(|config| config.state.as_deref()),
    )
    .map(PathBuf::from);
    let clustered_allow_ephemeral = flag_present(args, "allow-ephemeral-turbovec-cluster")
        || match opt(
            args,
            "allow-ephemeral-turbovec-cluster",
            "TURBOVEC_ALLOW_EPHEMERAL_CLUSTER",
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

    let dim = opt(
        args,
        "dim",
        "TURBOVEC_DIM",
        file.dim.map(|d| d.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "128".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid dim: {e}"))?;
    let bit_width = opt(
        args,
        "bit-width",
        "TURBOVEC_BIT_WIDTH",
        file.bit_width.map(|b| b.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "4".to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid bit width: {e}"))?;
    let vector_backend = opt(
        args,
        "vector-backend",
        "TURBOVEC_VECTOR_BACKEND",
        file.vector_backend.as_deref(),
    )
    .unwrap_or_else(|| crate::vector::EMBEDDED_TURBOVEC.to_string());

    // Shard set. CLI --index/--demo-vectors (with --node-listen and
    // --slot-offset) describes a single shard and overrides the file's
    // [[shards]] entirely; otherwise the file's shards are used.
    let node_listen_default = opt(
        args,
        "node-listen",
        "TURBOVEC_NODE_LISTEN",
        file.node_listen.as_deref(),
    )
    .unwrap_or_else(|| "0.0.0.0:50051".to_string());
    let cli_index = opt(args, "index", "TURBOVEC_INDEX", file.index.as_deref());
    let cli_demo = opt(
        args,
        "demo-vectors",
        "TURBOVEC_DEMO_VECTORS",
        file.demo_vectors.map(|d| d.to_string()).as_deref(),
    );
    let cli_offset = opt(
        args,
        "slot-offset",
        "TURBOVEC_SLOT_OFFSET",
        file.slot_offset.map(|o| o.to_string()).as_deref(),
    )
    .unwrap_or_else(|| "0".to_string())
    .parse::<u64>()
    .map_err(|e| format!("invalid slot offset: {e}"))?;

    // Write-ahead log: default on for persisted shards, always off for
    // demo shards (they have no index path to log next to). Bucket count
    // is a power of two (max 1024) and caps cheap split granularity.
    let wal_default = match opt(args, "wal", "TURBOVEC_WAL", None) {
        Some(s) => parse_env_bool(&s),
        None => file.wal.unwrap_or(true),
    };
    let wal_buckets_default = opt(
        args,
        "wal-buckets",
        "TURBOVEC_WAL_BUCKETS",
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
    let vocab_default = match opt(args, "vocab", "TURBOVEC_VOCAB", None) {
        Some(s) => parse_env_bool(&s),
        None => file.vocab.unwrap_or(false),
    };
    let vocab_window_docs = opt(
        args,
        "vocab-window-docs",
        "TURBOVEC_VOCAB_WINDOW_DOCS",
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
        "TURBOVEC_VOCAB_TOP_K",
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

    let chunk_blocks = opt(
        args,
        "chunk-blocks",
        "TURBOVEC_CHUNK_BLOCKS",
        file.chunk_blocks.map(|c| c.to_string()).as_deref(),
    )
    .unwrap_or_else(|| DEFAULT_CHUNK_BLOCKS.to_string())
    .parse::<usize>()
    .map_err(|e| format!("invalid chunk blocks: {e}"))?;

    let share_floors = match opt(args, "floor-sharing", "TURBOVEC_FLOOR_SHARING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.floor_sharing.unwrap_or(true),
    };
    let block_max = match opt(args, "block-max", "TURBOVEC_BLOCK_MAX", None) {
        Some(s) => parse_env_bool(&s),
        None => file.block_max.unwrap_or(true),
    };
    let segment_pruning = match opt(args, "segment-pruning", "TURBOVEC_SEGMENT_PRUNING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.segment_pruning.unwrap_or(true),
    };
    let shard_pruning = match opt(args, "shard-pruning", "TURBOVEC_SHARD_PRUNING", None) {
        Some(s) => parse_env_bool(&s),
        None => file.shard_pruning.unwrap_or(true),
    };
    let placement_column = opt(
        args,
        "placement-column",
        "TURBOVEC_PLACEMENT_COLUMN",
        file.placement_column.as_deref(),
    )
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty());
    let placement_leaf = opt(
        args,
        "placement-leaf",
        "TURBOVEC_PLACEMENT_LEAF",
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
        "TURBOVEC_PLACEMENT_TREE",
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
            "TURBOVEC_DERIVED_COLUMNS",
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
            "TURBOVEC_ALLOW_MISSING_BM25",
            None,
        ) {
            Some(s) => parse_env_bool(&s),
            None => file.allow_missing_bm25.unwrap_or(false),
        };
    let coalesce = match opt(args, "coalesce", "TURBOVEC_COALESCE", None) {
        Some(s) => parse_env_bool(&s),
        None => file.coalesce.unwrap_or(true),
    };
    let scan_parallel = opt(
        args,
        "scan-parallel",
        "TURBOVEC_SCAN_PARALLEL",
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
        "TURBOVEC_RERANK_PARALLEL",
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
        "TURBOVEC_FLOOR_DELTA",
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
        "TURBOVEC_FLOOR_WARMUP_CHUNKS",
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
        "TURBOVEC_FLOOR_MIN_INTERVAL_MS",
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
        "TURBOVEC_SHARD_DEADLINE_MS",
        file.shard_deadline_ms,
    )?;
    let hedge_delay_ms = parse_ms(
        "hedge-delay-ms",
        "TURBOVEC_HEDGE_DELAY_MS",
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
        "TURBOVEC_REPLICA_SYNC_MS",
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
        "TURBOVEC_REPLICA_STATE",
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
    let parse_control_u64 =
        |key: &str, env: &str, file_value: Option<u64>, default: u64| -> Result<u64, String> {
            opt(
                args,
                key,
                env,
                file_value.map(|value| value.to_string()).as_deref(),
            )
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --{key}: {error}"))
            })
            .transpose()
            .map(|value| value.unwrap_or(default))
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

    // Node membership (docs/cluster-control.md "Node lifecycle").
    let node_id = opt(
        args,
        "node-id",
        "PIPESTREAM_SEARCH_NODE_ID",
        file.node_id.as_deref(),
    );
    let control_addr = opt(
        args,
        "control-addr",
        "PIPESTREAM_SEARCH_CONTROL_ADDR",
        file.control_addr.as_deref(),
    );
    let failure_domain = opt(
        args,
        "failure-domain",
        "PIPESTREAM_SEARCH_FAILURE_DOMAIN",
        file.failure_domain.as_deref(),
    );
    let data_dir = opt(
        args,
        "data-dir",
        "PIPESTREAM_SEARCH_DATA_DIR",
        file.data_dir.as_deref(),
    );
    let advertise_addr = opt(
        args,
        "advertise-addr",
        "PIPESTREAM_SEARCH_ADVERTISE_ADDR",
        file.advertise_addr.as_deref(),
    );
    let replica_listen = opt(
        args,
        "replica-listen",
        "PIPESTREAM_SEARCH_REPLICA_LISTEN",
        file.replica_listen.as_deref(),
    )
    .map(|a| {
        a.parse::<SocketAddr>()
            .map_err(|e| format!("invalid --replica-listen: {e}"))
    })
    .transpose()?;
    let node_report_ms = parse_control_u64(
        "node-report-ms",
        "PIPESTREAM_SEARCH_NODE_REPORT_MS",
        file.node_report_ms,
        10_000,
    )?;
    let node_reconcile_ms = parse_control_u64(
        "node-reconcile-ms",
        "PIPESTREAM_SEARCH_NODE_RECONCILE_MS",
        file.node_reconcile_ms,
        2_000,
    )?;
    let node_lease_ms = parse_control_u64(
        "node-lease-ms",
        "PIPESTREAM_SEARCH_NODE_LEASE_MS",
        file.node_lease_ms,
        0,
    )?;
    let replica_lag_bound = parse_control_u64(
        "replica-lag-bound",
        "PIPESTREAM_SEARCH_REPLICA_LAG_BOUND",
        file.replica_lag_bound,
        0,
    )?;
    let membership = match node_id {
        Some(node_id) => {
            if node_id.trim().is_empty() {
                return Err("--node-id must not be empty".to_string());
            }
            if !matches!(role, Role::Node | Role::Both) {
                return Err(
                    "--node-id registers a node: it needs the node or both role".to_string()
                );
            }
            let control_addr = control_addr.ok_or_else(|| {
                "--node-id needs --control-addr (the coordinator's ClusterControl endpoint)"
                    .to_string()
            })?;
            let data_dir = data_dir.ok_or_else(|| {
                "--node-id needs --data-dir (where placed replicas live)".to_string()
            })?;
            if node_report_ms == 0 || node_reconcile_ms == 0 {
                return Err("--node-report-ms and --node-reconcile-ms must be positive".to_string());
            }
            if let Some(addr) = &advertise_addr {
                let well_formed = addr
                    .rsplit_once(':')
                    .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
                if !well_formed {
                    return Err(format!("--advertise-addr={addr:?} is not host:port"));
                }
            }
            Some(NodeMembershipConfig {
                node_id,
                control_addr: normalize_addr(control_addr),
                failure_domain: failure_domain.unwrap_or_default(),
                data_dir: PathBuf::from(data_dir),
                advertise_addr,
                replica_listen,
                report_ms: node_report_ms,
                reconcile_ms: node_reconcile_ms,
                lease_ms: node_lease_ms,
                lag_bound: replica_lag_bound,
            })
        }
        None => {
            if control_addr.is_some()
                || data_dir.is_some()
                || failure_domain.is_some()
                || advertise_addr.is_some()
                || replica_listen.is_some()
            {
                return Err(
                    "--control-addr, --data-dir, --failure-domain, --advertise-addr, and \
                     --replica-listen describe a registered node: pass --node-id with them"
                        .to_string(),
                );
            }
            None
        }
    };

    // The rest of the flags, the cross-checks, and the assembled
    // `Config` continue in `parse_tail`; the order of every check
    // (and so which error a bad command line names first) is unchanged.
    super::parse_tail::finish(
        args,
        file,
        super::parse_tail::Early {
            role,
            coord_listen,
            metrics_listen,
            shard_map_path,
            shard_map,
            shard_map_reload_ms,
            node_addrs,
            clustered_turbovec,
            dim,
            bit_width,
            vocab_window_docs,
            vocab_top_k,
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
        },
    )
}
