//! Tail catch-up and the single-image cutover under the write fences.

use std::time::Instant;

use tonic::Status;

use crate::node::{Bm25Shard, NodeServiceImpl, ShardState};
use crate::vector::VectorIndex;
use crate::wal::ClockedTail;

use super::{
    Analyze, Preflight, Shadow, CUTOVER_RETRIES, MAX_TAIL_PASSES, PAIRED_APPEND_PAUSE,
    PAIRED_APPEND_WAIT, STALLED_TAIL_PASSES,
};

#[path = "cutover_install.rs"]
mod install;

impl NodeServiceImpl {
    /// The tail loop and the cutover. Returns `(records applied under the
    /// lock, write-lock hold in ms, unlocked passes)`.
    pub(super) fn tail_and_cut_over(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
        flush_shadow: &mut dyn FnMut(&mut Shadow) -> Result<(), Status>,
    ) -> Result<(u64, u64, u64), Status> {
        let (mut tail, mut passes) = self.catch_up_compaction(pre, shadow, tail_bound, analyze)?;

        // Reserve commits before taking the seal lock, matching Flush's
        // order. Writers wait asynchronously while analysis and reads can
        // still run. The state lock is held only for the final fence/swap.
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self.seal_lock.lock().expect("seal lock poisoned");
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            // Analysis may use asynchronous I/O. A live-state lock here
            // can block the runtime workers needed to complete that I/O.
            self.apply_pass(shadow, records, analyze)?;
            passes += 1;
            // The shadow is private and the mutation/seal fences still exclude
            // ordinary commits. Sync it without blocking live readers, then
            // validate the live generation and watermark before installing it.
            // Defer this result so a rotated generation, live-WAL error or
            // watermark retry retains the same precedence as before.
            let shadow_sync = flush_shadow(shadow);
            let mut guard = crate::node::write_shard(&self.state);
            let started = Instant::now();
            if Self::flush_live_wal_locked(&mut guard, pre)? != watermark {
                drop(guard);
                if attempt >= CUTOVER_RETRIES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: the live WAL advanced during all \
                         {CUTOVER_RETRIES} cutover preparations"
                    )));
                }
                continue;
            }
            shadow_sync?;
            let previous = self.install(pre, shadow, &mut guard)?;
            let held = started.elapsed().as_millis() as u64;
            drop(guard);
            drop(previous);
            return Ok((0, held, passes));
        }
    }

    fn catch_up_compaction(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<(ClockedTail, u64), Status> {
        let mut tail = ClockedTail::start(&pre.gen_dir, pre.cutoff_clock);
        let mut passes = 0u64;
        let mut smallest = usize::MAX;
        let mut stalled = 0u64;
        loop {
            passes += 1;
            if passes > MAX_TAIL_PASSES {
                return Err(Status::resource_exhausted(format!(
                    "writes outpace compaction: {MAX_TAIL_PASSES} tail passes never left fewer \
                     than {tail_bound} records to apply"
                )));
            }
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            let count = records.len();
            self.apply_pass(shadow, records, analyze)?;
            if count < tail_bound {
                break;
            }
            if count < smallest {
                smallest = count;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled >= STALLED_TAIL_PASSES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: {STALLED_TAIL_PASSES} consecutive tail \
                         passes read no fewer than {smallest} records (last {count}, \
                         tail_bound {tail_bound}) after {passes} passes; pause writes or \
                         raise tail_bound"
                    )));
                }
            }
        }

        Ok((tail, passes))
    }

    // ----- The from-segments path: a catalog without a WAL -----
    //
    // docs/replay-from-segments.md, "Partitioned compaction of a catalog
    // without a log". The cutoff is the sealed catalog once the current
    // tail is sealed; the build transplants its live rows through
    // `reshard::compact_segments_partitioned` (the analyzer never runs);
    // writes that arrive after the cutoff are caught up by the same seal
    // ingest uses (`seal_tail`), landing in the live catalog as unordered
    // segments the cutover carries over; the cutover keeps the
    // shadow/marker/closing-flush contract of docs/mutations.md.

    /// Seal the tail when it holds rows (or a frozen part is waiting on
    /// its publication), waiting out a legacy two-RPC append caught
    /// mid-row the way the closing flush waits it out. Returns the rows
    /// the seal covered, 0 when there was nothing to seal.
    pub(super) fn seal_tail_wait(&self) -> Result<u64, Status> {
        let deadline = Instant::now() + PAIRED_APPEND_WAIT;
        loop {
            let (pending, frozen) = {
                let guard = crate::node::read_shard(&self.state);
                let (documents, vectors) = match guard.bm25.as_ref() {
                    Some(Bm25Shard::Segmented(shard)) => (
                        shard.tail().next_doc_id() as usize,
                        guard
                            .index
                            .as_ref()
                            .and_then(VectorIndex::as_segmented)
                            .map_or(0, |p| p.tail().len()),
                    ),
                    _ => (0, 0),
                };
                (
                    documents.max(vectors) as u64,
                    matches!(guard.bm25.as_ref(), Some(Bm25Shard::Segmented(shard)) if shard.frozen().is_some()),
                )
            };
            if pending == 0 && !frozen {
                return Ok(0);
            }
            // Paired: a fresh tail holding a document whose vectors have
            // not arrived yet is that same mid-row append; sealing it
            // would strand the document without its vectors.
            match self.seal_tail_paired() {
                Ok(_) => return Ok(pending),
                Err(status)
                    if Instant::now() < deadline
                        && status.code() == tonic::Code::FailedPrecondition
                        && status
                            .message()
                            .contains("a segment's artifacts cover the same rows") =>
                {
                    std::thread::sleep(PAIRED_APPEND_PAUSE);
                }
                Err(status) => return Err(status),
            }
        }
    }

    /// The tail holds documents whose vectors have not arrived: a
    /// two-call append between its calls, which only the writer can
    /// finish (see `ShardService::seal_tail_paired`).
    pub(super) fn tail_awaits_vectors(guard: &ShardState) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) else {
            return false;
        };
        shard.frozen().is_none() && shard.tail().next_doc_id() as usize > provider.tail().len()
    }

    /// The tail is empty on both legs and no seal is in flight.
    pub(super) fn segment_tail_is_clear(guard: &ShardState) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        if shard.tail().next_doc_id() != 0 || shard.frozen().is_some() {
            return false;
        }
        guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none_or(|p| p.tail().is_empty() && p.frozen().is_none())
    }
}
