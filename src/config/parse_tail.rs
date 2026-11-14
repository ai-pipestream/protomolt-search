use super::*;

/// The values `parse` has settled before its second half: everything
/// the remaining checks and the final `Config` read.
pub(super) struct Early {
    pub(super) role: Role,
    pub(super) coord_listen: SocketAddr,
    pub(super) metrics_listen: Option<SocketAddr>,
    pub(super) shard_map_path: Option<PathBuf>,
    pub(super) shard_map: Option<ShardMap>,
    pub(super) shard_map_reload_ms: u64,
    pub(super) node_addrs: Vec<String>,
    pub(super) clustered_turbovec: Option<ClusteredTurboVecConfig>,
    pub(super) dim: usize,
    pub(super) bit_width: usize,
    pub(super) vocab_window_docs: u64,
    pub(super) vocab_top_k: usize,
    pub(super) shards: Vec<ShardConfig>,
    pub(super) chunk_blocks: usize,
    pub(super) share_floors: bool,
    pub(super) block_max: bool,
    pub(super) segment_pruning: bool,
    pub(super) shard_pruning: bool,
    pub(super) placement_column: Option<String>,
    pub(super) placement_leaf: Option<i64>,
    pub(super) placement_tree: Option<std::sync::Arc<crate::placement::PinnedLeaf>>,
    pub(super) derived: Option<std::sync::Arc<crate::derived::Declaration>>,
    pub(super) allow_missing_bm25: bool,
    pub(super) coalesce: bool,
    pub(super) scan_parallel: usize,
    pub(super) rerank_parallel: usize,
    pub(super) floor_delta: f32,
    pub(super) floor_warmup_chunks: u32,
    pub(super) floor_min_interval_ms: u64,
    pub(super) shard_deadline_ms: u64,
    pub(super) hedge_delay_ms: u64,
    pub(super) replica_addrs: Vec<Option<String>>,
    pub(super) replica_sync_ms: u64,
    pub(super) replica_state_path: Option<PathBuf>,
    pub(super) control_state_path: Option<PathBuf>,
    pub(super) control_reconcile_ms: u64,
    pub(super) control_lease_ms: u64,
    pub(super) control_replication_factor: usize,
    pub(super) control_split_rows: u64,
    pub(super) control_merge_rows: u64,
    pub(super) control_compact_segments: u32,
    pub(super) control_compact_tombstone_ppm: u32,
    pub(super) membership: Option<NodeMembershipConfig>,
}

/// The second half of `parse`, from `--max-message-mib` to the
/// assembled `Config`.
pub(super) fn finish(args: &[String], file: FileConfig, early: Early) -> Result<Config, String> {
    let Early {
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
        mut shards,
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
    } = early;

    let max_message_bytes = opt(
        args,
        "max-message-mib",
        "TURBOVEC_MAX_MESSAGE_MIB",
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
            .or_else(|_| std::env::var("TURBOVEC_DEMO_QUERY"))
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.demo_query.unwrap_or(false);
    let query_dim = opt(
        args,
        "query-dim",
        "TURBOVEC_QUERY_DIM",
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

    let save_on_shutdown = match opt(args, "save-on-shutdown", "TURBOVEC_SAVE_ON_SHUTDOWN", None) {
        Some(s) => parse_env_bool(&s),
        None => file.save_on_shutdown.unwrap_or(true),
    };

    let stream_search = flag_present(args, "stream-search")
        || std::env::var("PIPESTREAM_SEARCH_STREAM_SEARCH")
            .or_else(|_| std::env::var("TURBOVEC_STREAM_SEARCH"))
            .map(|s| parse_env_bool(&s))
            .unwrap_or(false)
        || file.stream_search.unwrap_or(false);

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
            "TURBOVEC_BM25_STREAM",
            file.bm25_stream.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(true)
    };

    let max_k = opt(
        args,
        "max-k",
        "TURBOVEC_MAX_K",
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
        "TURBOVEC_SIGNAL_BATCH",
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
        "TURBOVEC_MAX_RERANK_MIB",
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
        "TURBOVEC_ANALYSIS_ADDR",
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
        "TURBOVEC_BM25_K1",
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
        "TURBOVEC_BM25_B",
        file.bm25_b.map(|v| v.to_string()).as_deref(),
    )
    .map(|s| s.parse::<f32>().map_err(|e| format!("invalid bm25 b: {e}")))
    .transpose()?
    .unwrap_or(0.75);

    let bm25_fields = match opt(args, "bm25-fields", "TURBOVEC_BM25_FIELDS", None) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file
            .bm25_fields
            .clone()
            .unwrap_or_else(|| vec!["body".to_string()]),
    };
    if bm25_fields.first().map(String::as_str) != Some("body") {
        return Err(
            "bm25 fields must start with \"body\" (field 0 is the stored body)".to_string(),
        );
    }
    for (i, name) in bm25_fields.iter().enumerate() {
        if bm25_fields[..i].contains(name) {
            return Err(format!("bm25 field {name:?} repeats in the field table"));
        }
    }

    let facet_fields: Vec<String> = match opt(args, "facet-fields", "TURBOVEC_FACET_FIELDS", None) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file.facet_fields.clone().unwrap_or_default(),
    };
    for (i, name) in facet_fields.iter().enumerate() {
        if facet_fields[..i].contains(name) {
            return Err(format!("facet field {name:?} repeats in the facet table"));
        }
    }

    let numeric_fields: Vec<String> =
        match opt(args, "numeric-fields", "TURBOVEC_NUMERIC_FIELDS", None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.numeric_fields.clone().unwrap_or_default(),
        };
    for (i, name) in numeric_fields.iter().enumerate() {
        if numeric_fields[..i].contains(name) {
            return Err(format!(
                "numeric field {name:?} repeats in the numeric table"
            ));
        }
        if facet_fields.contains(name) {
            return Err(format!(
                "column {name:?} is declared as both a facet and a numeric field; \
                 the v7 column table holds one column per name"
            ));
        }
    }

    let parse_list = |flag: &str, env: &str, file_val: &Option<Vec<String>>| -> Vec<String> {
        match opt(args, flag, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file_val.clone().unwrap_or_default(),
        }
    };
    let map_facet_fields = parse_list(
        "map-facet-fields",
        "TURBOVEC_MAP_FACET_FIELDS",
        &file.map_facet_fields,
    );
    let map_numeric_fields = parse_list(
        "map-numeric-fields",
        "TURBOVEC_MAP_NUMERIC_FIELDS",
        &file.map_numeric_fields,
    );
    let map_integer_fields = parse_list(
        "map-integer-fields",
        "PROTOMOLT_MAP_INTEGER_FIELDS",
        &file.map_integer_fields,
    );
    let map_unsigned_integer_fields = parse_list(
        "map-unsigned-integer-fields",
        "PROTOMOLT_MAP_UNSIGNED_INTEGER_FIELDS",
        &file.map_unsigned_integer_fields,
    );
    let integer_fields = parse_list(
        "integer-fields",
        "TURBOVEC_INTEGER_FIELDS",
        &file.integer_fields,
    );
    let unsigned_integer_fields = parse_list(
        "unsigned-integer-fields",
        "PIPESTREAM_SEARCH_UNSIGNED_INTEGER_FIELDS",
        &file.unsigned_integer_fields,
    );
    let geo_fields = parse_list("geo-fields", "TURBOVEC_GEO_FIELDS", &file.geo_fields);
    // One name space across all column kinds: the v7 column table
    // refuses duplicates, so the config does too, early and by name.
    // The placement column is an integer column whether or not the
    // integer list names it.
    {
        let mut all: Vec<&String> = Vec::new();
        let placement_as_integer: Vec<String> = placement_column
            .iter()
            .filter(|column| !integer_fields.contains(column))
            .cloned()
            .collect();
        for name in facet_fields
            .iter()
            .chain(&numeric_fields)
            .chain(&map_facet_fields)
            .chain(&map_numeric_fields)
            .chain(&map_integer_fields)
            .chain(&map_unsigned_integer_fields)
            .chain(&integer_fields)
            .chain(&unsigned_integer_fields)
            .chain(&placement_as_integer)
            .chain(&geo_fields)
        {
            if all.contains(&name) {
                return Err(format!(
                    "column {name:?} is declared under more than one column kind; \
                     the v7 column table holds one column per name"
                ));
            }
            all.push(name);
        }
    }

    let phrase_glossary = opt(
        args,
        "phrase-glossary",
        "PIPESTREAM_SEARCH_PHRASE_GLOSSARY",
        file.phrase_glossary.as_deref(),
    )
    .filter(|value| !value.trim().is_empty())
    .map(PathBuf::from);
    let phrase_field = opt(
        args,
        "phrase-field",
        "PIPESTREAM_SEARCH_PHRASE_FIELD",
        file.phrase_field.as_deref(),
    )
    .unwrap_or_else(|| "phrases".to_string());
    let entity_map_field = opt(
        args,
        "entity-map-field",
        "PIPESTREAM_SEARCH_ENTITY_MAP_FIELD",
        file.entity_map_field.as_deref(),
    )
    .filter(|value| !value.trim().is_empty());
    let phrase_ignore_case = opt(
        args,
        "phrase-ignore-case",
        "PIPESTREAM_SEARCH_PHRASE_IGNORE_CASE",
        file.phrase_ignore_case
            .map(|value| value.to_string())
            .as_deref(),
    )
    .map(|value| parse_env_bool(&value))
    .unwrap_or(true);
    let phrase_ner = if flag_present(args, "phrase-ner") {
        true
    } else {
        opt(
            args,
            "phrase-ner",
            "PIPESTREAM_SEARCH_PHRASE_NER",
            file.phrase_ner.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(false)
    };
    if phrase_glossary.is_some() {
        if phrase_field == "body" || !bm25_fields.contains(&phrase_field) {
            return Err(format!(
                "phrase glossary requires phrase field {phrase_field:?} as a non-body entry in --bm25-fields"
            ));
        }
        if let Some(field) = &entity_map_field {
            if !map_facet_fields.contains(field) {
                return Err(format!(
                    "entity map field {field:?} must be declared in --map-facet-fields"
                ));
            }
        }
        if phrase_ner && entity_map_field.is_none() {
            return Err("--phrase-ner requires --entity-map-field".to_string());
        }
    } else if entity_map_field.is_some() || phrase_ner {
        return Err("--entity-map-field and --phrase-ner require --phrase-glossary".to_string());
    }

    // Proximity payloads (docs/phrase-proximity.md). Both are explicit
    // storage declarations, like the phrase field: a positional field
    // must be in the table, and a bigram column is a declared table
    // entry named after its source, never an implicit extra field.
    let list = |name: &str, env: &str, file: Option<&Vec<String>>| -> Vec<String> {
        match opt(args, name, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.cloned().unwrap_or_default(),
        }
    };
    let position_fields = list(
        "position-fields",
        "PIPESTREAM_SEARCH_POSITION_FIELDS",
        file.position_fields.as_ref(),
    );
    let bigram_fields = list(
        "bigram-fields",
        "PIPESTREAM_SEARCH_BIGRAM_FIELDS",
        file.bigram_fields.as_ref(),
    );
    let sentence_fields = list(
        "sentence-fields",
        "PIPESTREAM_SEARCH_SENTENCE_FIELDS",
        file.sentence_fields.as_ref(),
    );
    for (i, name) in sentence_fields.iter().enumerate() {
        if sentence_fields[..i].contains(name) {
            return Err(format!(
                "sentence field {name:?} repeats in --sentence-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "sentence field {name:?} must be declared in --bm25-fields"
            ));
        }
        if name != "body" {
            return Err(format!(
                "sentence field {name:?}: snippets are cut from stored text, and only the body's text is stored; --sentence-fields accepts body only"
            ));
        }
    }
    for (i, name) in position_fields.iter().enumerate() {
        if position_fields[..i].contains(name) {
            return Err(format!(
                "positional field {name:?} repeats in --position-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "positional field {name:?} must be declared in --bm25-fields"
            ));
        }
        if phrase_glossary.is_some() && *name == phrase_field {
            return Err(format!(
                "phrase field {name:?} holds glossary concepts, not tokens; it cannot keep token positions"
            ));
        }
    }
    for (i, source) in bigram_fields.iter().enumerate() {
        let derived = crate::proximity::bigram_field_name(source);
        if bigram_fields[..i].contains(source) {
            return Err(format!(
                "bigram source {source:?} repeats in --bigram-fields"
            ));
        }
        if !bm25_fields.contains(source) {
            return Err(format!(
                "bigram source field {source:?} must be declared in --bm25-fields"
            ));
        }
        if !bm25_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} (derived from {source:?}) must be declared in --bm25-fields"
            ));
        }
        if crate::proximity::bigram_source(source).is_some() {
            return Err(format!(
                "bigram source {source:?} is itself a bigram column; columns derive from analyzed fields only"
            ));
        }
        if position_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} cannot keep token positions; it is a term column derived from {source:?}"
            ));
        }
        if phrase_glossary.is_some() && (*source == phrase_field || derived == phrase_field) {
            return Err(format!(
                "bigram source {source:?} and the phrase field {phrase_field:?} must be distinct"
            ));
        }
    }

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
    })
}
