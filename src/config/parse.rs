//! `parse`: resolve every option section, assemble the configuration, then
//! check the rules that span sections.

use super::columns::parse_column_fields;
use super::dataset::{resolve_collections, resolve_lexical, resolve_storage};
use super::file::read_file_config;
use super::membership::parse_membership;
use super::option::Options;
use super::phrase::parse_phrase_fields;
use super::placement::resolve_placement;
use super::raft::parse_raft;
use super::scan::{resolve_floors, resolve_limits, resolve_scan, resolve_stream};
use super::shards::{parse_clustered_turbovec, parse_shards, VectorShape};
use super::tls::resolve_security;
use super::topology::{resolve_control, resolve_topology};
use super::turbo::parse_turbo;
use super::validate::validate;
use super::Config;

/// The assembly table: the leading list names values resolved on their own,
/// then each section lends the `Config` fields it resolved, under the same names.
macro_rules! assemble {
    (own => [$($own:ident),+ $(,)?]; $($section:ident => [$($field:ident),+ $(,)?];)+) => {
        Config { $($own,)+ $($($field: $section.$field,)+)+ }
    };
}

/// Parse configuration from process args (excluding `argv[0]`).
pub fn parse(args: &[String]) -> Result<Config, String> {
    let o = Options::new(args);
    // The config file sits at the bottom of the precedence stack.
    let file = read_file_config(o)?;
    let topology = resolve_topology(o, &file)?;
    let control = resolve_control(o, &file, &topology)?;
    let clustered_turbovec = parse_clustered_turbovec(o, &file)?;
    let dim = o.value_or("dim", file.dim, 128)?;
    let bit_width = o.value_or("bit-width", file.bit_width, 4)?;
    let backend = o
        .text("vector-backend", file.vector_backend.as_deref())
        .unwrap_or_else(|| crate::vector::EMBEDDED_TURBOVEC.to_string());
    let shape = VectorShape {
        dim,
        bit_width,
        backend: &backend,
    };
    let shard_set = parse_shards(o, &file, shape)?;
    let scan = resolve_scan(o, &file)?;
    let floors = resolve_floors(o, &file)?;
    let placement = resolve_placement(o, &file)?;
    let membership = parse_membership(o, &file, topology.role)?;
    let limits = resolve_limits(o, &file, dim)?;
    let stream = resolve_stream(o, &file)?;
    let lexical = resolve_lexical(o, &file)?;
    let columns = parse_column_fields(o, &file, placement.placement_column.as_ref())?;
    let phrases = parse_phrase_fields(o, &file, &columns)?;
    let security = resolve_security(o, &file)?;
    let storage = resolve_storage(o, &file)?;
    let raft = parse_raft(o, &file, stream.relay)?;
    let turbo = parse_turbo(o, &file)?;
    let collections = Vec::new();
    let default_collection = file.default_collection.clone();
    let mut config = assemble! {
        own => [clustered_turbovec, bit_width, membership, raft, collections, default_collection];
        topology => [role, coord_listen, metrics_listen, node_addrs, replica_addrs, shard_map,
            shard_map_path, shard_map_reload_ms];
        control => [replica_sync_ms, replica_state_path, control_state_path, control_reconcile_ms,
            control_lease_ms, control_replication_factor, control_split_rows, control_merge_rows,
            control_compact_segments, control_compact_tombstone_ppm];
        shard_set => [shards, vocab_window_docs, vocab_top_k];
        scan => [chunk_blocks, share_floors, block_max, segment_pruning, shard_pruning,
            coalesce, scan_parallel, rerank_parallel];
        floors => [floor_delta, floor_warmup_chunks, floor_min_interval_ms, shard_deadline_ms,
            hedge_delay_ms];
        placement => [placement_column, placement_leaf, placement_tree, derived];
        limits => [max_message_bytes, max_k, signal_batch, max_rerank_bytes, demo_query,
            query_dim, save_on_shutdown];
        stream => [stream_search, stream_finish_settle_us, stream_finish_deadline_us,
            stream_poll_delay_us, relay, bm25_stream];
        lexical => [analysis_addr, bm25_k1, bm25_b, dense_quality_profile, synonyms,
            dense_execution_policy];
        columns => [bm25_fields, facet_fields, numeric_fields, map_facet_fields,
            map_numeric_fields, map_integer_fields, map_unsigned_integer_fields, integer_fields,
            unsigned_integer_fields, geo_fields];
        phrases => [phrase_glossary, phrase_field, entity_map_field, phrase_ignore_case,
            phrase_ner, position_fields, bigram_fields, sentence_fields];
        security => [tls, client_tls, allow_plaintext, principals, udp_hmac_key];
        storage => [seal_tail_docs, vector_mmap];
        turbo => [turbo_nlp_columns, turbo_one_pass_query, turbo_rerank_endpoint,
            turbo_rerank_model, turbo_matryoshka_dims, turbo_matryoshka_active_dim,
            turbo_vocab_endpoint, turbo_model_sha256, turbo_bearer_token];
    };
    // A single-shard setup shares the process's analysis backend with its shard.
    if let [shard] = config.shards.as_mut_slice() {
        if shard.analysis_addr.is_none() {
            shard.analysis_addr.clone_from(&config.analysis_addr);
        }
    }
    config.collections = resolve_collections(&file, &config)?;
    validate(&config)?;
    Ok(config)
}
