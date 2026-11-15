//! Scan execution, floor publication, request limits, and streaming options.

use crate::chunked::DEFAULT_CHUNK_BLOCKS;
use crate::MAX_MESSAGE_BYTES;

use super::file::FileConfig;
use super::option::Options;

/// How a node scans: chunking, floor sharing, pruning, batching, parallelism.
pub(super) struct Scan {
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

/// When floors go out, and how long a coordinator waits on a shard.
pub(super) struct Floors {
    pub(super) floor_delta: f32,
    pub(super) floor_warmup_chunks: u32,
    pub(super) floor_min_interval_ms: u64,
    pub(super) shard_deadline_ms: u64,
    pub(super) hedge_delay_ms: u64,
}

/// Request and message bounds, plus the startup demo query.
pub(super) struct Limits {
    pub(super) max_message_bytes: usize,
    pub(super) max_k: u32,
    pub(super) signal_batch: u32,
    pub(super) max_rerank_bytes: u64,
    pub(super) demo_query: bool,
    pub(super) query_dim: usize,
    pub(super) save_on_shutdown: bool,
}

/// The streaming protocols and the relay surface.
pub(super) struct Stream {
    pub(super) stream_search: bool,
    pub(super) stream_finish_settle_us: Option<u32>,
    pub(super) stream_finish_deadline_us: Option<u32>,
    pub(super) stream_poll_delay_us: Option<u64>,
    pub(super) relay: bool,
    pub(super) bm25_stream: bool,
}

pub(super) fn resolve_scan(o: Options, file: &FileConfig) -> Result<Scan, String> {
    let rerank_parallel = o.value_or("rerank-parallel", file.rerank_parallel, 0)?;
    if rerank_parallel > crate::node::MAX_RERANK_PARALLEL {
        return Err(format!(
            "--rerank-parallel must be <= {}, got {rerank_parallel}",
            crate::node::MAX_RERANK_PARALLEL
        ));
    }
    Ok(Scan {
        chunk_blocks: o.value_or("chunk-blocks", file.chunk_blocks, DEFAULT_CHUNK_BLOCKS)?,
        share_floors: o.switch("floor-sharing", file.floor_sharing, true),
        block_max: o.switch("block-max", file.block_max, true),
        segment_pruning: o.switch("segment-pruning", file.segment_pruning, true),
        shard_pruning: o.switch("shard-pruning", file.shard_pruning, true),
        allow_missing_bm25: o.switch_or_bare("allow-missing-bm25", file.allow_missing_bm25, false),
        coalesce: o.switch("coalesce", file.coalesce, true),
        scan_parallel: o.value_or("scan-parallel", file.scan_parallel, 0)?,
        rerank_parallel,
    })
}

pub(super) fn resolve_floors(o: Options, file: &FileConfig) -> Result<Floors, String> {
    let floor_delta = o.value_or("floor-delta", file.floor_delta, 0.0f32)?;
    if !(floor_delta.is_finite() && floor_delta >= 0.0) {
        return Err(format!(
            "--floor-delta must be finite and >= 0, got {floor_delta}"
        ));
    }
    Ok(Floors {
        floor_delta,
        floor_warmup_chunks: o.value_or("floor-warmup-chunks", file.floor_warmup_chunks, 0)?,
        floor_min_interval_ms: o.value_or(
            "floor-min-interval-ms",
            file.floor_min_interval_ms,
            0,
        )?,
        shard_deadline_ms: o.value_or("shard-deadline-ms", file.shard_deadline_ms, 0)?,
        hedge_delay_ms: o.value_or("hedge-delay-ms", file.hedge_delay_ms, 0)?,
    })
}

/// A positive count; zero would refuse every request it bounds.
fn positive_u32(flag: &str, value: u32, why: &str) -> Result<u32, String> {
    if value == 0 {
        return Err(format!("--{flag} must be at least 1 ({why})"));
    }
    Ok(value)
}

/// A MiB count turned into bytes, refused when zero or when it overflows.
fn mib_to_bytes(flag: &str, mib: u64) -> Result<u64, String> {
    if mib == 0 {
        return Err(format!("--{flag} must be positive"));
    }
    mib.checked_mul(1024 * 1024)
        .ok_or_else(|| format!("--{flag} {mib} overflows bytes"))
}

pub(super) fn resolve_limits(o: Options, file: &FileConfig, dim: usize) -> Result<Limits, String> {
    let max_message_bytes = match o.value::<usize>("max-message-mib", file.max_message_mib)? {
        Some(mib) => mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| format!("--max-message-mib {mib} overflows bytes"))?,
        None => MAX_MESSAGE_BYTES,
    };
    let max_k = match o.value("max-k", file.max_k)? {
        Some(k) => positive_u32("max-k", k, "0 would refuse every query")?,
        None => crate::coordinator::DEFAULT_MAX_K,
    };
    let signal_batch = match o.value("signal-batch", file.signal_batch)? {
        Some(n) => positive_u32("signal-batch", n, "ids per rescore call")?,
        None => crate::coordinator::DEFAULT_SIGNAL_BATCH,
    };
    let max_rerank_bytes = match o.value("max-rerank-mib", file.max_rerank_mib)? {
        Some(mib) => mib_to_bytes("max-rerank-mib", mib)?,
        None => crate::coordinator::DEFAULT_MAX_RERANK_BYTES,
    };
    Ok(Limits {
        max_message_bytes,
        max_k,
        signal_batch,
        max_rerank_bytes,
        demo_query: o.switch_or_bare("demo-query", file.demo_query, false),
        query_dim: o.value_or("query-dim", file.query_dim, dim)?,
        save_on_shutdown: o.switch("save-on-shutdown", file.save_on_shutdown, true),
    })
}

/// A stream window in microseconds, bounded by the dense policy's ceiling.
fn window_us(o: Options, flag: &str, file: Option<u32>) -> Result<Option<u32>, String> {
    let value = o.value::<u32>(flag, file)?;
    let ceiling = crate::dense_policy::MAX_FINISH_SETTLE_US;
    match value {
        Some(us) if !(1..=ceiling).contains(&us) => {
            Err(format!("--{flag} {us} is outside 1..={ceiling}"))
        }
        _ => Ok(value),
    }
}

pub(super) fn resolve_stream(o: Options, file: &FileConfig) -> Result<Stream, String> {
    Ok(Stream {
        stream_search: o.switch_or_bare("stream-search", file.stream_search, false),
        stream_finish_settle_us: window_us(
            o,
            "stream-finish-settle-us",
            file.stream_finish_settle_us,
        )?,
        stream_finish_deadline_us: window_us(
            o,
            "stream-finish-deadline-us",
            file.stream_finish_deadline_us,
        )?,
        stream_poll_delay_us: o.value("stream-poll-delay-us", file.stream_poll_delay_us)?,
        relay: o.switch_or_bare("relay", file.relay, false),
        bm25_stream: o.switch_or_bare("bm25-stream", file.bm25_stream, true),
    })
}
