//! Scan, floor, placement, streaming and limit knobs.

use std::sync::Arc;

use crate::chunked::DEFAULT_CHUNK_BLOCKS;
use crate::MAX_MESSAGE_BYTES;

use super::file::FileConfig;
use super::option::Options;
use super::shard_map::load_placement_tree;

/// How a shard scans and prunes.
pub(super) struct ScanKnobs {
    pub(super) chunk_blocks: usize,
    pub(super) share_floors: bool,
    pub(super) block_max: bool,
    pub(super) segment_pruning: bool,
    pub(super) shard_pruning: bool,
    pub(super) allow_missing_bm25: bool,
    pub(super) coalesce: bool,
    pub(super) scan_parallel: usize,
    pub(super) rerank_parallel: usize,
}

pub(super) fn resolve_scan(o: Options<'_>, file: &FileConfig) -> Result<ScanKnobs, String> {
    Ok(ScanKnobs {
        chunk_blocks: o
            .value("chunk-blocks", file.chunk_blocks)?
            .unwrap_or(DEFAULT_CHUNK_BLOCKS),
        share_floors: o.switch("floor-sharing", file.floor_sharing, true),
        block_max: o.switch("block-max", file.block_max, true),
        segment_pruning: o.switch("segment-pruning", file.segment_pruning, true),
        shard_pruning: o.switch("shard-pruning", file.shard_pruning, true),
        allow_missing_bm25: o.switch_or_bare("allow-missing-bm25", file.allow_missing_bm25, false),
        coalesce: o.switch("coalesce", file.coalesce, true),
        scan_parallel: o.value("scan-parallel", file.scan_parallel)?.unwrap_or(0),
        rerank_parallel: o
            .value("rerank-parallel", file.rerank_parallel)?
            .unwrap_or(0),
    })
}

/// Floor sharing cadence and the per-shard deadlines.
pub(super) struct FloorKnobs {
    pub(super) floor_delta: f32,
    pub(super) floor_warmup_chunks: u32,
    pub(super) floor_min_interval_ms: u64,
    pub(super) shard_deadline_ms: u64,
    pub(super) hedge_delay_ms: u64,
}

pub(super) fn resolve_floors(o: Options<'_>, file: &FileConfig) -> Result<FloorKnobs, String> {
    let floor_delta = o
        .value::<f32>("floor-delta", file.floor_delta)?
        .unwrap_or(0.0);
    if !floor_delta.is_finite() || floor_delta < 0.0 {
        return Err(format!(
            "floor delta must be finite and >= 0, got {floor_delta}"
        ));
    }
    Ok(FloorKnobs {
        floor_delta,
        floor_warmup_chunks: o
            .value("floor-warmup-chunks", file.floor_warmup_chunks)?
            .unwrap_or(0),
        floor_min_interval_ms: o
            .value("floor-min-interval-ms", file.floor_min_interval_ms)?
            .unwrap_or(0),
        shard_deadline_ms: o
            .value("shard-deadline-ms", file.shard_deadline_ms)?
            .unwrap_or(0),
        hedge_delay_ms: o.value("hedge-delay-ms", file.hedge_delay_ms)?.unwrap_or(0),
    })
}

/// The placement a shard pins and the derived columns it computes.
pub(super) struct Placement {
    pub(super) column: Option<String>,
    pub(super) leaf: Option<i64>,
    pub(super) tree: Option<Arc<crate::placement::PinnedLeaf>>,
    pub(super) derived: Option<Arc<crate::derived::Declaration>>,
}

pub(super) fn resolve_placement(o: Options<'_>, file: &FileConfig) -> Result<Placement, String> {
    let column = o.trimmed("placement-column", file.placement_column.as_deref());
    let leaf = o.value::<i64>("placement-leaf", file.placement_leaf)?;
    if let Some(code) = leaf.filter(|code| *code < 0) {
        return Err(format!(
            "placement leaf {code} is negative; codes are non-negative path codes"
        ));
    }
    let tree = match o.trimmed("placement-tree", file.placement_tree.as_deref()) {
        Some(path) => {
            let (Some(column), Some(code)) = (column.as_deref(), leaf) else {
                return Err(
                    "--placement-tree needs --placement-leaf (and --placement-column): the \
                     tree is checked on the leaf this shard pins"
                        .to_string(),
                );
            };
            let tree = load_placement_tree(std::path::Path::new(&path))?;
            let pinned = crate::placement::PinnedLeaf::pin(&tree, column, code)
                .map_err(|e| format!("placement tree {path}: {e}"))?;
            Some(Arc::new(pinned))
        }
        None => None,
    };
    Ok(Placement {
        column,
        leaf,
        tree,
        derived: derived_declaration(o, file)?,
    })
}

/// The derived-column declaration (docs/derived-columns.md): a file named
/// by `--derived-columns`, or the config file's own `[[derived]]` table,
/// never both.
fn derived_declaration(
    o: Options<'_>,
    file: &FileConfig,
) -> Result<Option<Arc<crate::derived::Declaration>>, String> {
    let path = o.trimmed("derived-columns", file.derived_columns.as_deref());
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
            .map(Arc::new)
            .map_err(|e| format!("derived columns: {e}"))
    })
    .transpose()
}

/// The streamed search protocol and the relay switch.
pub(super) struct StreamKnobs {
    pub(super) stream_search: bool,
    pub(super) finish_settle_us: Option<u32>,
    pub(super) finish_deadline_us: Option<u32>,
    pub(super) poll_delay_us: Option<u64>,
    pub(super) relay: bool,
    pub(super) bm25_stream: bool,
}

pub(super) fn resolve_stream(o: Options<'_>, file: &FileConfig) -> Result<StreamKnobs, String> {
    let window = |flag: &str, from_file: Option<u32>| -> Result<Option<u32>, String> {
        let value = o.value::<u32>(flag, from_file)?;
        match value {
            Some(us) if !(1..=crate::dense_policy::MAX_FINISH_SETTLE_US).contains(&us) => {
                Err(format!(
                    "--{flag} {us} is outside 1..={}",
                    crate::dense_policy::MAX_FINISH_SETTLE_US
                ))
            }
            _ => Ok(value),
        }
    };
    Ok(StreamKnobs {
        stream_search: o.raised("stream-search", file.stream_search),
        finish_settle_us: window("stream-finish-settle-us", file.stream_finish_settle_us)?,
        finish_deadline_us: window("stream-finish-deadline-us", file.stream_finish_deadline_us)?,
        poll_delay_us: o.value("stream-poll-delay-us", file.stream_poll_delay_us)?,
        relay: o.raised("relay", file.relay),
        bm25_stream: o.switch_or_bare("bm25-stream", file.bm25_stream, true),
    })
}

/// Request and message limits, and the demo switches.
pub(super) struct Limits {
    pub(super) max_k: u32,
    pub(super) signal_batch: u32,
    pub(super) max_rerank_bytes: u64,
    pub(super) max_message_bytes: usize,
    pub(super) demo_query: bool,
    pub(super) query_dim: usize,
    pub(super) save_on_shutdown: bool,
}

pub(super) fn resolve_limits(
    o: Options<'_>,
    file: &FileConfig,
    dim: usize,
) -> Result<Limits, String> {
    let max_k = o
        .value::<u32>("max-k", file.max_k)?
        .unwrap_or(crate::coordinator::DEFAULT_MAX_K);
    if max_k == 0 {
        return Err("max k must be at least 1 (0 would refuse every query)".to_string());
    }
    let signal_batch = o
        .value::<u32>("signal-batch", file.signal_batch)?
        .unwrap_or(crate::coordinator::DEFAULT_SIGNAL_BATCH);
    if signal_batch == 0 {
        return Err("signal batch must be at least 1 (ids per rescore call)".to_string());
    }
    let max_rerank_bytes = match o.value::<u64>("max-rerank-mib", file.max_rerank_mib)? {
        Some(0) => return Err("max rerank MiB must be positive".to_string()),
        Some(mib) => mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| "max rerank MiB overflows bytes".to_string())?,
        None => crate::coordinator::DEFAULT_MAX_RERANK_BYTES,
    };
    Ok(Limits {
        max_k,
        signal_batch,
        max_rerank_bytes,
        max_message_bytes: o
            .value::<usize>("max-message-mib", file.max_message_mib)?
            .map_or(MAX_MESSAGE_BYTES, |mib| mib * 1024 * 1024),
        demo_query: o.raised("demo-query", file.demo_query),
        query_dim: o.value("query-dim", file.query_dim)?.unwrap_or(dim),
        save_on_shutdown: o.switch("save-on-shutdown", file.save_on_shutdown, true),
    })
}
