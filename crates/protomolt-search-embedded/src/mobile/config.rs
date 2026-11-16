//! Mobile open requests translated into the embedded runtime's config.

use std::path::PathBuf;

use pipestream_search::embedded::{
    EmbeddedDocumentCatalogConfig, EmbeddedSearchConfig, EmbeddedShardConfig,
};
use pipestream_search::pb::mobile::{MobileOpenRequest, MobileShardConfig};

use super::bridge::BridgeError;

/// A persisted shard is a segment catalog, which has no interrupted bulk
/// build to serve around; the flag is refused rather than ignored.
fn refuse_missing_bm25_override(config: &MobileShardConfig) -> Result<(), BridgeError> {
    if config.allow_missing_bm25 {
        return Err(BridgeError::invalid(
            "allow_missing_bm25 has no effect: a persisted shard is a segment catalog, which \
             has no interrupted bulk build to serve around",
        ));
    }
    Ok(())
}

pub(super) fn node_config(config: MobileShardConfig) -> Result<EmbeddedShardConfig, BridgeError> {
    refuse_missing_bm25_override(&config)?;
    if config.in_memory && !config.index_path.is_empty() {
        return Err(BridgeError::invalid(
            "mobile shard cannot set both in_memory and index_path",
        ));
    }
    let mut shard = if config.in_memory {
        EmbeddedShardConfig::in_memory(config.slot_offset)
    } else if config.index_path.is_empty() {
        return Err(BridgeError::invalid(
            "mobile persisted shard requires index_path",
        ));
    } else {
        EmbeddedShardConfig::persistent(PathBuf::from(config.index_path), config.slot_offset)
    };
    let node = &mut shard.node;
    if let Some(value) = config.vector_backend {
        node.vector_backend = value;
    }
    if let Some(value) = config.bit_width {
        node.bit_width = value as usize;
    }
    if !config.bm25_fields.is_empty() {
        node.bm25_fields = config.bm25_fields;
    }
    node.facet_fields = config.facet_fields;
    node.numeric_fields = config.numeric_fields;
    node.map_facet_fields = config.map_facet_fields;
    node.map_numeric_fields = config.map_numeric_fields;
    node.map_integer_fields = config.map_integer_fields;
    node.map_unsigned_integer_fields = config.map_unsigned_integer_fields;
    node.integer_fields = config.integer_fields;
    node.unsigned_integer_fields = config.unsigned_integer_fields;
    node.geo_fields = config.geo_fields;
    if let Some(value) = config.wal {
        node.wal = value;
    }
    if let Some(value) = config.wal_buckets {
        node.wal_buckets = value;
    }
    if let Some(value) = config.vocab {
        node.vocab = value;
    }
    if let Some(value) = config.vocab_window_docs {
        node.vocab_window_docs = value;
    }
    if let Some(value) = config.vocab_top_k {
        node.vocab_top_k = value as usize;
    }
    if let Some(value) = config.chunk_blocks {
        node.chunk_blocks = value as usize;
    }
    if let Some(value) = config.share_floors {
        node.share_floors = value;
    }
    if let Some(value) = config.block_max {
        node.block_max = value;
    }
    if let Some(value) = config.floor_delta {
        node.floor_delta = value;
    }
    if let Some(value) = config.floor_warmup_chunks {
        node.floor_warmup_chunks = value;
    }
    if let Some(value) = config.floor_min_interval_ms {
        node.floor_min_interval_ms = value;
    }
    if let Some(value) = config.coalesce {
        node.coalesce = value;
    }
    if let Some(value) = config.scan_parallel {
        node.scan_parallel = value as usize;
    }
    if let Some(value) = config.rerank_parallel {
        node.rerank_parallel = value as usize;
    }
    Ok(shard)
}

pub(super) fn embedded_config(
    request: MobileOpenRequest,
) -> Result<EmbeddedSearchConfig, BridgeError> {
    if request.shards.is_empty() {
        return Err(BridgeError::invalid(
            "mobile open request requires at least one shard",
        ));
    }
    let shards = request
        .shards
        .into_iter()
        .map(node_config)
        .collect::<Result<Vec<_>, _>>()?;
    let mut config = EmbeddedSearchConfig::new(shards);
    if let Some(catalog) = request.document_catalog {
        if catalog.in_memory != catalog.path.is_empty() {
            return Err(BridgeError::invalid(
                "document catalog path must be empty exactly when in_memory is true",
            ));
        }
        for shard in &mut config.shards {
            shard.node.collection = catalog.collection.clone();
        }
        config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
            collection: catalog.collection,
            path: (!catalog.in_memory).then(|| PathBuf::from(catalog.path)),
        });
    }
    if let Some(value) = request.bm25_k1 {
        config.bm25_params.k1 = value;
    }
    if let Some(value) = request.bm25_b {
        config.bm25_params.b = value;
    }
    if let Some(value) = request.stream_search {
        config.stream_search = value;
    }
    if let Some(value) = request.bm25_stream {
        config.bm25_stream = value;
    }
    if let Some(value) = request.max_k {
        config.max_k = value;
    }
    if let Some(value) = request.max_rerank_bytes {
        config.max_rerank_bytes = value;
    }
    config.topology_generation = request.topology_generation;
    Ok(config)
}
