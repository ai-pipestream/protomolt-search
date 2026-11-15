//! `parse`: resolve every option section, assemble the configuration, then
//! check the rules that span sections.

use super::columns::{parse_column_fields, parse_phrase_fields};
use super::dataset::{resolve_collections, resolve_lexical, resolve_storage};
use super::file::read_file_config;
use super::membership::parse_membership;
use super::option::Options;
use super::raft::parse_raft;
use super::scan::{
    resolve_floors, resolve_limits, resolve_placement, resolve_scan, resolve_stream,
};
use super::shards::{parse_clustered_turbovec, parse_shards, VectorShape};
use super::tls::resolve_security;
use super::topology::{resolve_control, resolve_topology};
use super::turbo::parse_turbo;
use super::validate::validate;
use super::Config;

/// Parse configuration from process args (excluding `argv[0]`).
pub fn parse(args: &[String]) -> Result<Config, String> {
    let o = Options::new(args);
    // The config file sits at the bottom of the precedence stack.
    let file = read_file_config(o)?;
    let topology = resolve_topology(o, &file)?;
    let control = resolve_control(o, &file, &topology)?;
    let clustered_turbovec = parse_clustered_turbovec(o, &file)?;
    let dim = o.value::<usize>("dim", file.dim)?.unwrap_or(128);
    let bit_width = o.value::<usize>("bit-width", file.bit_width)?.unwrap_or(4);
    let vector_backend = o
        .text("vector-backend", file.vector_backend.as_deref())
        .unwrap_or_else(|| crate::vector::EMBEDDED_TURBOVEC.to_string());
    let shape = VectorShape {
        dim,
        bit_width,
        backend: &vector_backend,
    };
    let shard_set = parse_shards(o, &file, shape)?;
    let scan = resolve_scan(o, &file)?;
    let floors = resolve_floors(o, &file)?;
    let placement = resolve_placement(o, &file)?;
    let membership = parse_membership(o, &file, topology.role)?;
    let limits = resolve_limits(o, &file, dim)?;
    let stream = resolve_stream(o, &file)?;
    let lexical = resolve_lexical(o, &file)?;
    let columns = parse_column_fields(o, &file, &placement.column)?;
    let phrases = parse_phrase_fields(o, &file, &columns.bm25_fields, &columns.map_facet_fields)?;
    let security = resolve_security(o, &file)?;
    let storage = resolve_storage(o, &file)?;
    let raft = parse_raft(o, &file, stream.relay)?;
    let turbo = parse_turbo(o, &file)?;
    let mut config = Config {
        role: topology.role,
        coord_listen: topology.coord_listen,
        raft,
        metrics_listen: topology.metrics_listen,
        node_addrs: topology.node_addrs,
        clustered_turbovec,
        shards: shard_set.shards,
        chunk_blocks: scan.chunk_blocks,
        share_floors: scan.share_floors,
        block_max: scan.block_max,
        segment_pruning: scan.segment_pruning,
        shard_pruning: scan.shard_pruning,
        placement_column: placement.column,
        placement_leaf: placement.leaf,
        placement_tree: placement.tree,
        derived: placement.derived,
        allow_missing_bm25: scan.allow_missing_bm25,
        coalesce: scan.coalesce,
        scan_parallel: scan.scan_parallel,
        rerank_parallel: scan.rerank_parallel,
        floor_delta: floors.floor_delta,
        floor_warmup_chunks: floors.floor_warmup_chunks,
        floor_min_interval_ms: floors.floor_min_interval_ms,
        shard_deadline_ms: floors.shard_deadline_ms,
        hedge_delay_ms: floors.hedge_delay_ms,
        replica_addrs: topology.replica_addrs,
        max_message_bytes: limits.max_message_bytes,
        demo_query: limits.demo_query,
        stream_search: stream.stream_search,
        stream_finish_settle_us: stream.finish_settle_us,
        stream_finish_deadline_us: stream.finish_deadline_us,
        stream_poll_delay_us: stream.poll_delay_us,
        relay: stream.relay,
        bm25_stream: stream.bm25_stream,
        max_k: limits.max_k,
        signal_batch: limits.signal_batch,
        max_rerank_bytes: limits.max_rerank_bytes,
        dense_quality_profile: lexical.dense_quality_profile,
        synonyms: lexical.synonyms,
        dense_execution_policy: lexical.dense_execution_policy,
        query_dim: limits.query_dim,
        bit_width,
        save_on_shutdown: limits.save_on_shutdown,
        analysis_addr: lexical.analysis_addr,
        bm25_k1: lexical.bm25_k1,
        bm25_b: lexical.bm25_b,
        bm25_fields: columns.bm25_fields,
        facet_fields: columns.facet_fields,
        numeric_fields: columns.numeric_fields,
        map_facet_fields: columns.map_facet_fields,
        phrase_glossary: phrases.phrase_glossary,
        phrase_field: phrases.phrase_field,
        entity_map_field: phrases.entity_map_field,
        phrase_ignore_case: phrases.phrase_ignore_case,
        phrase_ner: phrases.phrase_ner,
        position_fields: phrases.position_fields,
        bigram_fields: phrases.bigram_fields,
        sentence_fields: phrases.sentence_fields,
        map_numeric_fields: columns.map_numeric_fields,
        map_integer_fields: columns.map_integer_fields,
        map_unsigned_integer_fields: columns.map_unsigned_integer_fields,
        integer_fields: columns.integer_fields,
        unsigned_integer_fields: columns.unsigned_integer_fields,
        geo_fields: columns.geo_fields,
        shard_map: topology.shard_map,
        shard_map_path: topology.shard_map_path,
        shard_map_reload_ms: topology.shard_map_reload_ms,
        replica_sync_ms: control.replica_sync_ms,
        replica_state_path: control.replica_state_path,
        control_state_path: control.state_path,
        control_reconcile_ms: control.reconcile_ms,
        control_lease_ms: control.lease_ms,
        control_replication_factor: control.replication_factor,
        control_split_rows: control.split_rows,
        control_merge_rows: control.merge_rows,
        control_compact_segments: control.compact_segments,
        control_compact_tombstone_ppm: control.compact_tombstone_ppm,
        membership,
        collections: Vec::new(),
        default_collection: file.default_collection.clone(),
        tls: security.tls,
        client_tls: security.client_tls,
        allow_plaintext: security.allow_plaintext,
        principals: security.principals,
        udp_hmac_key: security.udp_hmac_key,
        layout: storage.layout,
        seal_tail_docs: storage.seal_tail_docs,
        vector_mmap: storage.vector_mmap,
        vocab_window_docs: shard_set.vocab_window_docs,
        vocab_top_k: shard_set.vocab_top_k,
        turbo_nlp_columns: turbo.turbo_nlp_columns,
        turbo_one_pass_query: turbo.turbo_one_pass_query,
        turbo_rerank_endpoint: turbo.turbo_rerank_endpoint,
        turbo_rerank_model: turbo.turbo_rerank_model,
        turbo_matryoshka_dims: turbo.turbo_matryoshka_dims,
        turbo_matryoshka_active_dim: turbo.turbo_matryoshka_active_dim,
        turbo_vocab_endpoint: turbo.turbo_vocab_endpoint,
        turbo_model_sha256: turbo.turbo_model_sha256,
        turbo_bearer_token: turbo.turbo_bearer_token,
    };
    // A single-shard setup shares the process's analysis backend with its shard.
    if config.shards.len() == 1 && config.shards[0].analysis_addr.is_none() {
        config.shards[0]
            .analysis_addr
            .clone_from(&config.analysis_addr);
    }
    config.collections = resolve_collections(&file, &config)?;
    validate(&config)?;
    Ok(config)
}
