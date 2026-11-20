//! Synchronized shard state, stats-epoch guards, and runtime-safe lock helpers.

use crate::node::*;

mod reads;

/// Corpus shares after tombstones and the mandatory document view.
pub(in crate::node) fn live_field_stats(
    index: &dyn Bm25Index,
    terms: &[String],
    live_docs: &LiveDocs,
    slots: u32,
    visibility: Option<&crate::boolean_bits::Bits>,
) -> (u64, Vec<u32>) {
    if let Some(visibility) = visibility {
        let admitted =
            |slot: u32| !live_docs.is_deleted(slot as usize) && visibility.test(slot as usize);
        let total_doc_length = (0..slots)
            .filter(|slot| admitted(*slot))
            .map(|slot| u64::from(index.doc_length(slot)))
            .sum();
        let dfs = terms
            .iter()
            .map(|term| {
                let mut df = 0;
                index.for_each_doc_tf(term, &mut |doc, _| {
                    if admitted(doc) {
                        df += 1;
                    }
                });
                df
            })
            .collect();
        return (total_doc_length, dfs);
    }
    if !live_docs.has_deletes() {
        return (
            index.total_doc_length(),
            terms.iter().map(|term| index.df(term)).collect(),
        );
    }
    // Retain the unrestricted path's sparse tombstone length subtraction;
    // summing live lengths would fault in every mapped length page needlessly.
    let deleted_length: u64 = (0..slots)
        .filter(|slot| live_docs.is_deleted(*slot as usize))
        .map(|slot| u64::from(index.doc_length(slot)))
        .sum();
    let dfs = terms
        .iter()
        .map(|term| {
            let mut live_df = 0u32;
            index.for_each_doc_tf(term, &mut |doc, _tf| {
                if !live_docs.is_deleted(doc as usize) {
                    live_df += 1;
                }
            });
            live_df
        })
        .collect();
    (index.total_doc_length().saturating_sub(deleted_length), dfs)
}

pub(in crate::node) fn live_document_count(
    store: &Bm25Shard,
    live_docs: &LiveDocs,
    visibility: Option<&crate::boolean_bits::Bits>,
) -> u64 {
    if !live_docs.has_deletes() && visibility.is_none() {
        return store.doc_count();
    }
    let views: Vec<_> = (0..store.field_count())
        .filter_map(|field| store.field_view(field))
        .collect();
    (0..store.next_doc_id())
        .filter(|slot| {
            !live_docs.is_deleted(*slot as usize)
                && visibility.is_none_or(|allow| allow.test(*slot as usize))
                && views.iter().any(|view| view.doc_length(*slot) > 0)
        })
        .count() as u64
}

pub(in crate::node) fn active_artifact_rows(guard: &ShardState) -> Vec<u64> {
    [
        guard.index.as_ref().map(|index| index.len() as u64),
        guard
            .bm25
            .as_ref()
            .map(|store| u64::from(store.next_doc_id())),
        guard.exact_vectors.as_ref().map(|store| store.len() as u64),
    ]
    .into_iter()
    .flatten()
    .filter(|rows| *rows != 0)
    .collect()
}

pub(crate) fn physical_rows(guard: &ShardState) -> u64 {
    active_artifact_rows(guard).into_iter().max().unwrap_or(0)
}

/// Synchronized shard state holding the vector index, BM25 store, tombstones,
/// WAL, and stats-epoch fence.
#[derive(Default)]
pub(crate) struct ShardState {
    pub(crate) index: Option<VectorIndex>,
    /// Row-major FP32 vectors aligned one-for-one with provider slots.
    pub(crate) exact_vectors: Option<ExactVectorStore>,
    /// Generation-bound multi-resolution rows swapped atomically on publication.
    pub(crate) multi_res_vectors: crate::multi_res::MultiResVectors,
    pub(crate) bm25: Option<Bm25Shard>,
    /// Generation tombstones shared by every lexical and vector read path.
    pub(crate) live_docs: LiveDocs,
    /// Active snapshot generation directory when installed via `InstallSnapshot`.
    pub(crate) generation: Option<PathBuf>,
    /// Write-ahead log (`<index path>.wal/`), or `None` when WAL is disabled.
    pub(crate) wal: Option<WalWriter>,
    /// Cached slot -> parent map for collapse scans (lineage parent_id
    /// per slot), bound to the physical version that supplied the lineage.
    pub(crate) parents: Option<ParentMap>,
    /// RAM authority for the mapped-plan identity every mapped stream must
    /// match; loaded on attach, recorded in WAL markers, persisted at flush.
    pub(crate) mapped_binding: Option<crate::postings::StoredBinding>,
    /// Monotonic BM25 mutation counter enforced via `TermStats` claims; starts
    /// at 1 (`0` means no claim).
    pub(crate) stats_epoch: u64,
    /// True only after Flush persisted the current mutation state.
    pub(crate) files_current: bool,
    pub(crate) stats_incarnation: crate::stats_identity::StatsIncarnation,
    /// Compaction cutover awaiting its closing flush; rolls back on restart
    /// until `Flush` retires its marker.
    pub(crate) pending_compaction: Option<crate::compaction::PendingCommit>,
}

#[derive(Clone)]
pub(crate) struct ParentMap {
    pub(in crate::node) claim: crate::stats_identity::StatsClaim,
    pub(in crate::node) rows: Arc<Vec<u64>>,
}

impl ShardState {
    pub(crate) fn check_legacy_mutation(&self) -> Result<(), Status> {
        self.check_catalog_publication()?;
        if matches!(self.bm25.as_ref(), Some(Bm25Shard::Segmented(shard))
            if shard.snapshot().manifest().source_owner.is_some()
                || shard.catalog().snapshot().manifest().source_owner.is_some())
        {
            return Err(Status::failed_precondition("source-managed index requires its owning source publication journal; legacy mutation and index-only snapshots are unavailable"));
        }
        Ok(())
    }

    pub(in crate::node) fn check_catalog_publication(&self) -> Result<(), Status> {
        if matches!(self.bm25.as_ref(), Some(Bm25Shard::Segmented(shard))
            if shard.catalog().publication_is_uncertain())
        {
            return Err(Status::failed_precondition(
                "segment publication is uncertain; reopen for recovery before flushing or sealing",
            ));
        }
        Ok(())
    }

    /// Never wrap a mutation version into one a coordinator may still hold.
    pub(crate) fn advance_stats_epoch(&mut self) {
        self.files_current = false;
        if let Some(next) = self.stats_epoch.checked_add(1) {
            self.stats_epoch = next;
        } else {
            self.stats_incarnation = Default::default();
            self.stats_epoch = 1;
        }
    }

    pub(in crate::node) fn publish_parent_map(&mut self, built: &ParentMap) -> Result<(), Status> {
        self.check_stats_epoch(built.claim.epoch, &built.claim.incarnation())?;
        self.parents = Some(built.clone());
        Ok(())
    }

    /// Enforce a scoring request's stats-epoch claim (see
    /// `Bm25QueryRequest.expected_stats_epoch`). Must be called under
    /// the same guard the scoring reads through — checking on one guard
    /// acquisition and scoring on another would leave a gap an ingest
    /// commit can slip into.
    pub(in crate::node) fn check_stats_epoch(
        &self,
        expected: u64,
        incarnation: &[u8],
    ) -> Result<(), Status> {
        self.stats_incarnation
            .check(self.stats_epoch, expected, incarnation)
    }
}

impl ShardState {
    /// Materializes a single-pass [`crate::pb::RerankCandidatePayload`] (stored
    /// document text, lineage, and stored sentence spans) for `local_slot` under
    /// the caller's existing shard read guard, avoiding any Stage-2
    /// `GetDocuments` round-trip.
    pub(crate) fn rerank_candidate_for(
        &self,
        local_slot: u32,
    ) -> Option<crate::pb::RerankCandidatePayload> {
        if self.live_docs.is_deleted(local_slot as usize) {
            return None;
        }
        let store = self.bm25.as_ref()?.as_index()?;
        let text = store.text(local_slot)?;
        let lineage = store.lineage(local_slot).map(|l| crate::pb::DocLineage {
            parent_id: l.parent_id,
            group_id: l.group_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        let sentence_spans = store
            .doc_sentences(local_slot)
            .unwrap_or_default()
            .into_iter()
            .map(|(start, end)| crate::pb::OffsetSpan { start, end })
            .collect();
        Some(crate::pb::RerankCandidatePayload {
            text,
            lineage,
            sentence_spans,
        })
    }
}

/// RAII release for the ingest gate.
pub(in crate::node) struct IngestGuard(pub(in crate::node) Arc<std::sync::atomic::AtomicBool>);

impl Drop for IngestGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Acquire a blocking `ShardState` lock via `tokio::task::block_in_place` when
/// called on a multi-threaded Tokio worker so contended acquisitions yield the
/// worker's task queue rather than starving async analysis streams.
fn wait_off_runtime<T>(wait: impl FnOnce() -> T) -> T {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// The shard read lock; see [`wait_off_runtime`].
pub(crate) fn read_shard(state: &RwLock<ShardState>) -> std::sync::RwLockReadGuard<'_, ShardState> {
    match state.try_read() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            panic!("shard state lock poisoned: {poisoned}")
        }
        Err(std::sync::TryLockError::WouldBlock) => {
            wait_off_runtime(|| state.read().expect("shard state lock poisoned"))
        }
    }
}

/// The shard write lock; see [`wait_off_runtime`].
pub(crate) fn write_shard(
    state: &RwLock<ShardState>,
) -> std::sync::RwLockWriteGuard<'_, ShardState> {
    match state.try_write() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            panic!("shard state lock poisoned: {poisoned}")
        }
        Err(std::sync::TryLockError::WouldBlock) => {
            wait_off_runtime(|| state.write().expect("shard state lock poisoned"))
        }
    }
}
