//! Row top-k collection and the public streaming search entrypoint.

use super::*;

impl CoordinatorServiceImpl {
    /// [`Self::fanout_search`] over the streaming protocol
    /// (`NodeService.StreamSearch`): shards hold no top-k and emit
    /// every candidate at or above the live floor; the heap here — the
    /// only one in the system — defines k. Whenever its k-th best
    /// tightens, `floor_seed(kth)` (one f32 ULP below, so boundary ties
    /// survive) is pushed to every open stream.
    ///
    /// Exactness: the successful path never sends Stop, every shard's
    /// terminal summary must certify `completed = true`, every emission
    /// scored at or above the floor in effect when its block was scanned,
    /// and every pushed floor is a lower bound on the global k-th best. An
    /// error cancels unfinished streams, whose summaries are necessarily
    /// incomplete and unusable. Successful results are identical to
    /// [`Self::fanout_search`] (same scores, same `merge_topk` total order).
    ///
    /// `initial_floor` seeds every shard's starting floor — the hybrid
    /// seam, where a finished BM25 leg's decomposed floor prunes the
    /// vector scan from the first block.
    pub async fn fanout_stream_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        initial_floor: Option<f32>,
        filters: &RequestFilters,
    ) -> Result<StreamFanoutResult, Status> {
        let result = self
            .run_stream_query(super::stream_run::StreamQuery {
                request_id,
                vector,
                k,
                initial_floor,
                filters,
                collapse: false,
            })
            .await?;
        Ok(StreamFanoutResult {
            hits: result.hits,
            summaries: result.summaries,
            floors_sent: result.floors_sent,
            shards_finished_early: result.shards_finished_early,
            shards_floor_pruned: result.shards_floor_pruned,
        })
    }
}

pub(super) struct RowCollector {
    heap: std::collections::BinaryHeap<StreamHeapEntry>,
    seen: std::collections::HashSet<(u32, u64)>,
}

impl RowCollector {
    pub(super) fn new(k: u32) -> Self {
        Self {
            heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
            seen: std::collections::HashSet::with_capacity(k as usize * 2),
        }
    }

    pub(super) fn is_full(&self, k: u32) -> bool {
        self.heap.len() == k as usize
    }

    pub(super) fn snapshot(&self) -> Vec<(u64, f32)> {
        self.heap
            .iter()
            .map(|entry| (entry.0.vector_id, entry.0.score))
            .collect()
    }

    pub(super) fn batch(
        &mut self,
        shard: usize,
        bytes: &[u8],
        k: u32,
    ) -> Result<super::stream_run::BatchUpdate, Status> {
        if !bytes.len().is_multiple_of(12) {
            return Err(Status::internal(format!(
                "shard {shard} sent a misaligned batch of {} bytes",
                bytes.len()
            )));
        }
        let mut changed = false;
        for rec in bytes.as_chunks::<12>().0 {
            let vector_id = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
            if !self.seen.insert((shard as u32, vector_id)) {
                continue;
            }
            let entry = StreamHeapEntry(MergedHit {
                vector_id,
                shard: shard as u32,
                score: f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score")),
            });
            if self.heap.len() < k as usize {
                self.heap.push(entry);
                changed = true;
            } else if cmp_hits(&entry.0, &self.heap.peek().expect("heap is full").0)
                == std::cmp::Ordering::Less
            {
                self.heap.pop();
                self.heap.push(entry);
                changed = true;
            }
        }
        let full = self.is_full(k);
        Ok(super::stream_run::BatchUpdate {
            full,
            changed,
            kth: full.then(|| self.heap.peek().expect("heap is full").0.score),
        })
    }

    pub(super) fn finish(self) -> Vec<MergedHit> {
        let mut all: Vec<MergedHit> = self.heap.into_iter().map(|e| e.0).collect();
        all.sort_by(cmp_hits);
        all
    }
}
