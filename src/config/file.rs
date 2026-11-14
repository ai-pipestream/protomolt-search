//! The TOML config file: the bottom of the precedence stack.

use serde::Deserialize;

use super::args::opt;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct FileCollection {
    pub(super) name: String,
    pub(super) nodes: Option<Vec<String>>,
    pub(super) shard_map: Option<String>,
    pub(super) analysis_addr: Option<String>,
    pub(super) bm25_k1: Option<f32>,
    pub(super) bm25_b: Option<f32>,
    pub(super) dense_quality_profile: Option<String>,
    pub(super) synonyms: Option<String>,
    pub(super) dense_execution_policy: Option<String>,
    pub(super) replica_state: Option<String>,
    pub(super) control_state: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct FileConfig {
    pub(super) collections: Vec<FileCollection>,
    pub(super) default_collection: Option<String>,
    pub(super) tls_cert: Option<String>,
    pub(super) tls_key: Option<String>,
    pub(super) tls_client_ca: Option<String>,
    pub(super) tls_ca: Option<String>,
    pub(super) raft_dir: Option<String>,
    pub(super) raft_node_id: Option<u64>,
    pub(super) raft_group_id: Option<String>,
    pub(super) raft_authority_incarnation: Option<String>,
    pub(super) raft_listen: Option<String>,
    pub(super) raft_advertise: Option<String>,
    pub(super) raft_peers: Option<String>,
    pub(super) raft_heartbeat_ms: Option<u64>,
    pub(super) raft_election_min_ms: Option<u64>,
    pub(super) raft_election_max_ms: Option<u64>,
    pub(super) raft_lease_ms: Option<u64>,
    pub(super) raft_skew_ms: Option<u64>,
    pub(super) raft_max_snapshot_bytes: Option<u64>,
    pub(super) raft_map_principal: Option<String>,
    pub(super) raft_map_workspace: Option<String>,
    pub(super) raft_map_collection: Option<String>,
    pub(super) raft_managed_catalog: Option<Vec<String>>,
    pub(super) raft_managed_principal: Option<String>,
    pub(super) raft_managed_max_request_bytes: Option<u64>,
    pub(super) raft_managed_max_pending_bytes: Option<u64>,
    pub(super) raft_managed_max_in_flight: Option<u64>,
    pub(super) tls_client_cert: Option<String>,
    pub(super) tls_client_key: Option<String>,
    pub(super) tls_domain: Option<String>,
    pub(super) allow_plaintext: Option<bool>,
    pub(super) bearer_tokens: Option<String>,
    pub(super) udp_hmac_key: Option<String>,
    pub(super) layout: Option<String>,
    pub(super) seal_tail_docs: Option<u32>,
    pub(super) vector_mmap: Option<bool>,
    pub(super) role: Option<String>,
    pub(super) node_listen: Option<String>,
    pub(super) coord_listen: Option<String>,
    pub(super) metrics_listen: Option<String>,
    pub(super) nodes: Option<Vec<String>>,
    pub(super) clustered_turbovec: Option<FileClusteredTurboVec>,
    pub(super) index: Option<String>,
    pub(super) slot_offset: Option<u64>,
    pub(super) demo_vectors: Option<usize>,
    pub(super) dim: Option<usize>,
    pub(super) bit_width: Option<usize>,
    pub(super) vector_backend: Option<String>,
    pub(super) chunk_blocks: Option<usize>,
    pub(super) floor_sharing: Option<bool>,
    pub(super) block_max: Option<bool>,
    pub(super) segment_pruning: Option<bool>,
    pub(super) shard_pruning: Option<bool>,
    pub(super) placement_column: Option<String>,
    pub(super) placement_leaf: Option<i64>,
    pub(super) placement_tree: Option<String>,
    /// `--derived-columns=<file>`: a shard map or a file with a
    /// `[[derived]]` table.
    pub(super) derived_columns: Option<String>,
    /// The declaration inline (`[[derived]]` in this file).
    pub(super) derived: Vec<crate::derived::DerivedColumnConfig>,
    pub(super) allow_missing_bm25: Option<bool>,
    pub(super) coalesce: Option<bool>,
    pub(super) scan_parallel: Option<usize>,
    pub(super) rerank_parallel: Option<usize>,
    pub(super) floor_delta: Option<f32>,
    pub(super) floor_warmup_chunks: Option<u32>,
    pub(super) floor_min_interval_ms: Option<u64>,
    pub(super) shard_deadline_ms: Option<u64>,
    pub(super) hedge_delay_ms: Option<u64>,
    pub(super) max_message_mib: Option<usize>,
    pub(super) demo_query: Option<bool>,
    pub(super) stream_search: Option<bool>,
    pub(super) stream_finish_settle_us: Option<u32>,
    pub(super) stream_finish_deadline_us: Option<u32>,
    pub(super) stream_poll_delay_us: Option<u64>,
    pub(super) relay: Option<bool>,
    pub(super) bm25_stream: Option<bool>,
    pub(super) max_k: Option<u32>,
    pub(super) signal_batch: Option<u32>,
    pub(super) max_rerank_mib: Option<u64>,
    pub(super) dense_quality_profile: Option<String>,
    pub(super) synonyms: Option<String>,
    pub(super) dense_execution_policy: Option<String>,
    pub(super) query_dim: Option<usize>,
    pub(super) save_on_shutdown: Option<bool>,
    pub(super) analysis_addr: Option<String>,
    pub(super) bm25_k1: Option<f32>,
    pub(super) bm25_b: Option<f32>,
    pub(super) bm25_fields: Option<Vec<String>>,
    pub(super) facet_fields: Option<Vec<String>>,
    pub(super) numeric_fields: Option<Vec<String>>,
    pub(super) map_facet_fields: Option<Vec<String>>,
    pub(super) phrase_glossary: Option<String>,
    pub(super) phrase_field: Option<String>,
    pub(super) entity_map_field: Option<String>,
    pub(super) phrase_ignore_case: Option<bool>,
    pub(super) phrase_ner: Option<bool>,
    pub(super) position_fields: Option<Vec<String>>,
    pub(super) bigram_fields: Option<Vec<String>>,
    pub(super) sentence_fields: Option<Vec<String>>,
    pub(super) map_numeric_fields: Option<Vec<String>>,
    pub(super) map_integer_fields: Option<Vec<String>>,
    pub(super) map_unsigned_integer_fields: Option<Vec<String>>,
    pub(super) integer_fields: Option<Vec<String>>,
    pub(super) unsigned_integer_fields: Option<Vec<String>>,
    pub(super) geo_fields: Option<Vec<String>>,
    pub(super) wal: Option<bool>,
    pub(super) wal_buckets: Option<u32>,
    pub(super) vocab: Option<bool>,
    pub(super) vocab_window_docs: Option<u64>,
    pub(super) vocab_top_k: Option<usize>,
    pub(super) turbo_nlp_columns_json: Option<String>,
    pub(super) turbo_one_pass_query: Option<bool>,
    pub(super) turbo_rerank_endpoint: Option<String>,
    pub(super) turbo_rerank_model: Option<String>,
    pub(super) turbo_matryoshka_dims: Option<Vec<usize>>,
    pub(super) turbo_matryoshka_active_dim: Option<usize>,
    pub(super) turbo_vocab_endpoint: Option<String>,
    pub(super) turbo_model_sha256: Option<String>,
    pub(super) turbo_bearer_token: Option<String>,
    pub(super) shard_map: Option<String>,
    pub(super) shard_map_reload_ms: Option<u64>,
    pub(super) replica_sync_ms: Option<u64>,
    pub(super) replica_state: Option<String>,
    pub(super) control_state: Option<String>,
    pub(super) control_reconcile_ms: Option<u64>,
    pub(super) control_lease_ms: Option<u64>,
    pub(super) control_replication_factor: Option<usize>,
    pub(super) control_split_rows: Option<u64>,
    pub(super) control_merge_rows: Option<u64>,
    pub(super) control_compact_segments: Option<u32>,
    pub(super) control_compact_tombstone_ppm: Option<u32>,
    pub(super) node_id: Option<String>,
    pub(super) control_addr: Option<String>,
    pub(super) failure_domain: Option<String>,
    pub(super) data_dir: Option<String>,
    pub(super) advertise_addr: Option<String>,
    pub(super) replica_listen: Option<String>,
    pub(super) node_report_ms: Option<u64>,
    pub(super) node_reconcile_ms: Option<u64>,
    pub(super) node_lease_ms: Option<u64>,
    pub(super) replica_lag_bound: Option<u64>,
    pub(super) shards: Vec<FileShard>,
}

/// `[clustered_turbovec]` accepts exactly one of `coordinator` (external) or
/// `nodes` (embedded coordinator).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct FileClusteredTurboVec {
    pub(super) coordinator: Option<String>,
    pub(super) nodes: Option<Vec<String>>,
    pub(super) state: Option<String>,
    pub(super) allow_ephemeral: Option<bool>,
}

/// One `[[shards]]` table in the TOML file.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct FileShard {
    pub(super) collection: Option<String>,
    pub(super) listen: Option<String>,
    pub(super) vector_backend: Option<String>,
    pub(super) index: Option<String>,
    pub(super) slot_offset: Option<u64>,
    pub(super) demo_vectors: Option<usize>,
    pub(super) analysis_addr: Option<String>,
    pub(super) wal: Option<bool>,
    pub(super) wal_buckets: Option<u32>,
    pub(super) vocab: Option<bool>,
    pub(super) shard_id: Option<String>,
    pub(super) hash_lo: Option<u64>,
    pub(super) hash_hi: Option<u64>,
}

/// The config file at the bottom of the precedence stack, shared by
/// the serving parse and the local directory commands.
pub(super) fn read_file_config(args: &[String]) -> Result<FileConfig, String> {
    match opt(args, "config", "TURBOVEC_CONFIG", None) {
        Some(path) => {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("read config {path}: {e}"))?;
            toml::from_str(&text).map_err(|e| format!("parse config {path}: {e}"))
        }
        None => Ok(FileConfig::default()),
    }
}
