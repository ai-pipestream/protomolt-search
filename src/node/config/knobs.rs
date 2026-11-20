//! Runtime and startup-fixed diagnostic knobs derived from the node configuration.

use crate::node::*;

/// Build runtime knobs from `config`: live atomics plus startup fixed entries.
pub(in crate::node) fn node_knobs(config: &NodeConfig) -> crate::diagnostics::Knobs {
    use crate::diagnostics::{Knobs, NodeKnobValues};
    let process = format!("node:{}", config.slot_offset);
    let mut fixed = core_fixed_knobs(config);
    fixed.extend(shard_and_placement_fixed_knobs(config));
    Knobs::node(
        process,
        NodeKnobValues {
            share_floors: config.share_floors,
            segment_pruning: config.segment_pruning,
            floor_delta: config.floor_delta,
            floor_warmup_chunks: config.floor_warmup_chunks,
            floor_min_interval_ms: config.floor_min_interval_ms,
        },
        fixed,
    )
}

fn core_fixed_knobs(config: &NodeConfig) -> Vec<crate::diagnostics::FixedKnob> {
    use crate::diagnostics::FixedKnob;
    use crate::pb::KnobKind;
    vec![
        FixedKnob {
            name: "score_splade_max_k",
            kind: KnobKind::Int,
            value: config.max_k.to_string(),
            description: "Largest ScoreSplade k admitted by this node (--max-k).",
        },
        FixedKnob {
            name: "chunk_blocks",
            kind: KnobKind::Int,
            value: config.chunk_blocks.to_string(),
            description: "SIMD blocks per scan chunk (--chunk-blocks).",
        },
        FixedKnob {
            name: "block_max",
            kind: KnobKind::Bool,
            value: config.block_max.to_string(),
            description: "Block-max pruning on the lexical leg (--block-max).",
        },
        FixedKnob {
            name: "coalesce",
            kind: KnobKind::Bool,
            value: config.coalesce.to_string(),
            description: "Coalesce concurrent scans into one pass (--coalesce).",
        },
        FixedKnob {
            name: "scan_parallel",
            kind: KnobKind::Int,
            value: config.scan_parallel.to_string(),
            description: "Scan slots (--scan-parallel).",
        },
        FixedKnob {
            name: "rerank_parallel",
            kind: KnobKind::Int,
            value: config.rerank_parallel.to_string(),
            description: "Exact rerank slots (--rerank-parallel).",
        },
        FixedKnob {
            name: "layout",
            kind: KnobKind::String,
            value: match config.layout {
                Layout::Segments => "segments".to_string(),
                Layout::SingleImage => "single-image".to_string(),
            },
            description: "On-disk layout of a new persisted shard (--layout).",
        },
        FixedKnob {
            name: "vector_mmap",
            kind: KnobKind::Bool,
            value: config.vector_mmap.to_string(),
            description: "Serve sealed vector images through a memory map (--vector-mmap).",
        },
        FixedKnob {
            name: "seal_tail_docs",
            kind: KnobKind::Int,
            value: config.seal_tail_docs.to_string(),
            description: "Rows that seal the tail into a segment (--seal-tail-docs).",
        },
        FixedKnob {
            name: "bit_width",
            kind: KnobKind::Int,
            value: config.bit_width.to_string(),
            description: "Quantized bits per dimension (--bit-width).",
        },
    ]
}

fn shard_and_placement_fixed_knobs(config: &NodeConfig) -> Vec<crate::diagnostics::FixedKnob> {
    use crate::diagnostics::FixedKnob;
    use crate::pb::KnobKind;
    vec![
        FixedKnob {
            name: "slot_offset",
            kind: KnobKind::Int,
            value: config.slot_offset.to_string(),
            description: "First global id of this shard.",
        },
        FixedKnob {
            name: "collection",
            kind: KnobKind::String,
            value: config.collection.clone(),
            description: "The collection this shard belongs to.",
        },
        FixedKnob {
            name: "vector_backend",
            kind: KnobKind::String,
            value: config.vector_backend.clone(),
            description: "The vector backend the shard was built with.",
        },
        FixedKnob {
            name: "placement_column",
            kind: KnobKind::String,
            value: config.placement_column.clone().unwrap_or_default(),
            description: "The i64 column that holds each row's placement code \
                          (--placement-column).",
        },
        FixedKnob {
            name: "placement_leaf",
            kind: KnobKind::String,
            value: config
                .placement_leaf
                .map(|code| code.to_string())
                .unwrap_or_default(),
            description: "The placement code this shard serves (--placement-leaf).",
        },
        FixedKnob {
            name: "placement_tree",
            kind: KnobKind::String,
            value: config
                .placement_tree
                .as_ref()
                .map(|pinned| pinned.leaf().name.clone())
                .unwrap_or_default(),
            description: "The pinned leaf's name under the tree this shard checks direct \
                          rows against (--placement-tree); empty without a tree.",
        },
        FixedKnob {
            name: "derived_columns",
            kind: KnobKind::String,
            value: config
                .derived
                .as_ref()
                .map(|declaration| {
                    crate::derived::declaration_toml(declaration.spec()).unwrap_or_default()
                })
                .unwrap_or_default(),
            description: "The index's derived-column declaration as a [[derived]] table \
                          (--derived-columns); empty without one.",
        },
        FixedKnob {
            name: "derived_fingerprint",
            kind: KnobKind::String,
            value: config
                .derived
                .as_ref()
                .map(|declaration| declaration.fingerprint().to_string())
                .unwrap_or_default(),
            description: "The declaration's fingerprint, which the store, the log and every \
                          segment of this shard carry.",
        },
    ]
}
