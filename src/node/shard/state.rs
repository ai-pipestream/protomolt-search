//! Synchronized shard state, stats-epoch guards, and runtime-safe lock helpers.

use crate::node::*;

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

/// Message prefix of a stats-epoch refusal. The coordinator's retry
/// distinguishes this refusal from every other `FAILED_PRECONDITION` by
/// this prefix, so it and [`ShardState::check_stats_epoch`] move together.
pub(crate) const STALE_STATS_EPOCH: &str = "stale stats epoch";

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

    /// Resolve from the durable generation, never from a caller's field grant.
    pub(in crate::node) fn vector_binding(
        &self,
        field: &str,
    ) -> Result<Option<crate::pb::MappedVectorBinding>, Status> {
        let binding = self
            .mapped_binding
            .as_ref()
            .map(|stored| {
                crate::mapped_vector::decode(&stored.vector_binding, &stored.plan_fingerprint)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "invalid stored vector binding: {}",
                            error.message()
                        ))
                    })
            })
            .transpose()?
            .flatten();
        if !field.is_empty() && binding.as_ref().is_none_or(|held| held.field != field) {
            return Err(Status::failed_precondition(
                "requested vector field is not bound on this generation",
            ));
        }
        if let Some(binding) = &binding {
            if binding.declared_dimensions != 0 {
                for dim in [
                    self.index.as_ref().and_then(|index| index.dim_opt()),
                    self.exact_vectors.as_ref().and_then(|exact| exact.dim()),
                ]
                .into_iter()
                .flatten()
                {
                    if dim != binding.declared_dimensions as usize {
                        return Err(Status::failed_precondition(
                            "vector shape disagrees with its durable field binding",
                        ));
                    }
                }
            }
        }
        Ok(binding)
    }

    /// Capture proof of the physical view before any scan data is emitted.
    pub(in crate::node) fn vector_read_receipt(
        &self,
        context: Option<&crate::pb::VectorReadContext>,
    ) -> Result<Option<crate::pb::VectorReadReceipt>, Status> {
        let Some(context) = context else {
            return Ok(None);
        };
        let scope = crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
        self.check_stats_epoch(
            context.expected_stats_epoch,
            &context.expected_stats_incarnation,
        )?;
        let (_, visibility_columns_known) = filter_known_flags(
            self.bm25.as_ref(),
            &[],
            context
                .visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        Ok(Some(crate::pb::VectorReadReceipt {
            vector_binding: self.vector_binding(&context.field)?,
            stats_epoch: self.stats_epoch,
            stats_incarnation: self.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        }))
    }

    pub(in crate::node) fn vector_scan_filters(
        &self,
        rows: usize,
        geo_filters: &[crate::pb::GeoFilter],
        geo_regions: &[crate::geo::GeoRegion],
        user: Option<&crate::pb::FilterExpr>,
        context: Option<&crate::pb::VectorReadContext>,
        prune: bool,
    ) -> Result<(ResolvedShardFilters, Option<crate::pb::VectorReadReceipt>), Status> {
        let receipt = self.vector_read_receipt(context)?;
        let visibility = context.and_then(|context| context.visibility.as_ref());
        let combined = crate::visibility::intersect_filter(visibility, user.cloned())?;
        let (filter, mut allow, stats) = resolve_shard_filters(
            self.bm25.as_ref(),
            self.live_docs.words(),
            rows,
            geo_filters,
            geo_regions,
            combined.as_ref(),
            prune,
            None,
        )?;
        if visibility.is_some() {
            // An absent authority column is not permission to include a vector
            // with no document metadata, even for an IS MISSING predicate.
            let documents = self
                .bm25
                .as_ref()
                .map_or(0, |store| store.next_doc_id() as usize);
            let allow = allow.get_or_insert_with(|| crate::boolean_bits::Bits::full(rows));
            allow.clear_range(documents, rows);
        }
        Ok(((filter, allow, stats), receipt))
    }

    /// Evaluate authority predicates only for requested live slots. Candidate
    /// bookkeeping stays proportional to the request, not the whole corpus.
    pub(in crate::node) fn visible_candidate_slots(
        &self,
        visibility: Option<&crate::pb::DocumentVisibility>,
        ids: &[u64],
        offset: u64,
        rows: usize,
    ) -> Result<Vec<usize>, Status> {
        let restricted = visibility.is_some();
        let store = self.bm25.as_ref();
        if restricted && store.is_some_and(|store| store.as_index().is_none()) {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush before scoped candidate scoring",
            ));
        }
        let predicate = visibility.and_then(|view| view.filter.as_ref());
        let filter = crate::filter::DocFilter {
            pred: match (predicate, store) {
                (Some(predicate), Some(store)) => Some(store.resolve_filter(predicate)?),
                _ => None,
            },
            ..Default::default()
        };
        let mut seen = std::collections::HashSet::new();
        Ok(ids
            .iter()
            .filter_map(|&id| {
                let slot = usize::try_from(id.checked_sub(offset)?).ok()?;
                if slot >= rows || self.live_docs.is_deleted(slot) || !seen.insert(slot) {
                    return None;
                }
                if restricted
                    && store.is_none_or(|store| {
                        slot >= store.next_doc_id() as usize
                            || !filter.passes(slot as u32, &ShardNumericRead(store))
                    })
                {
                    return None;
                }
                Some(slot)
            })
            .collect())
    }

    /// Check under the postings read guard. A binding also pins fields with
    /// no rows yet; a field name alone does not establish term identity.
    pub(in crate::node) fn check_query_analysis(
        &self,
        field: &str,
        offered: u64,
    ) -> Result<(), Status> {
        let explicit = self
            .mapped_binding
            .as_ref()
            .filter(|binding| !binding.analysis_sha.is_empty());
        let bound = match explicit {
            Some(binding) => crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )
            .map_err(Status::failed_precondition)?
            .fields
            .into_iter()
            .find(|column| column.name == field)
            .map(|column| crate::analyzer::analysis_fingerprint(column.analysis.as_ref())),
            None => None,
        };
        let held = bound
            .or_else(|| {
                self.bm25.as_ref().and_then(|store| {
                    store
                        .field_index(field)
                        .map(|fi| store.analysis_fingerprint(fi))
                })
            })
            .unwrap_or(0);
        if held != 0 && ((explicit.is_some() && offered == 0) || (offered != 0 && held != offered))
        {
            let reason = if offered == 0 {
                "explicit mapped analysis cannot be omitted"
            } else {
                "the two score different term identities"
            };
            return Err(Status::failed_precondition(format!(
                "field {field:?} was built with analyzer fingerprint {held:#x} but the query's terms were analyzed under {offered:#x}; {reason}"
            )));
        }
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
