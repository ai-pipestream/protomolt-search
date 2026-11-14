//! Parent grouping and the public collapsed streaming search entrypoint.

use super::*;

impl CoordinatorServiceImpl {
    /// Document-mode streaming: [`Self::fanout_stream_search`] with
    /// `k` meaning k distinct PARENT documents. Shards emit chunks
    /// tagged with their parents (lineage `parent_id`, or tagged
    /// self-parents); the coordinator owns the whole document
    /// aggregation — a parent's score is its best chunk's, the floor
    /// is one ULP below the k-th best parent score, and each returned
    /// parent carries EVERY chunk at or above the final floor,
    /// whichever shards held them. No colocation: an opinion whose
    /// chunks straddle a shard cut aggregates here exactly like one
    /// whose chunks share a shard.
    ///
    /// Lossless: a chunk below the parent floor can neither beat its
    /// own parent's best nor introduce a new top-k parent, and every
    /// chunk scoring at or above the FINAL floor cleared every earlier
    /// (lower) floor, so filtering the retained chunks to the final
    /// floor makes the groups exact and layout-invariant.
    pub async fn fanout_stream_search_collapse(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        filters: &RequestFilters,
    ) -> Result<CollapseStreamResult, Status> {
        let result = self
            .run_stream_query(super::stream_run::StreamQuery {
                request_id,
                vector,
                k,
                initial_floor: None,
                filters,
                collapse: true,
            })
            .await?;
        Ok(CollapseStreamResult {
            hits: result.hits,
            groups: result.groups,
            chunk_floor: result.chunk_floor,
            summaries: result.summaries,
            floors_sent: result.floors_sent,
            shards_finished_early: result.shards_finished_early,
            shards_floor_pruned: result.shards_floor_pruned,
        })
    }
}

struct ParentAgg {
    best_score: f32,
    best_id: u64,
    chunks: Vec<(u64, f32)>,
}

pub(super) struct ParentCollector {
    parents: HashMap<u64, ParentAgg>,
    seen: std::collections::HashSet<(u32, u64)>,
    kth: f32,
}

impl ParentCollector {
    pub(super) fn new(k: u32) -> Self {
        Self {
            parents: HashMap::new(),
            seen: std::collections::HashSet::with_capacity(k as usize * 2),
            kth: f32::NEG_INFINITY,
        }
    }

    pub(super) fn is_full(&self, k: u32) -> bool {
        self.parents.len() >= k as usize
    }

    pub(super) fn batch(
        &mut self,
        shard: usize,
        bytes: &[u8],
        k: u32,
    ) -> Result<super::stream_run::BatchUpdate, Status> {
        if !bytes.len().is_multiple_of(20) {
            return Err(Status::internal(format!(
                "shard {shard} sent a misaligned collapse batch of {} bytes",
                bytes.len()
            )));
        }
        let mut dirty = false;
        for rec in bytes.as_chunks::<20>().0 {
            let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
            if !self.seen.insert((shard as u32, doc)) {
                continue;
            }
            let score = f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
            let parent = u64::from_le_bytes(rec[12..20].try_into().expect("8-byte parent"));
            let agg = self.parents.entry(parent).or_insert_with(|| {
                dirty = true;
                ParentAgg {
                    best_score: f32::NEG_INFINITY,
                    best_id: u64::MAX,
                    chunks: Vec::new(),
                }
            });
            agg.chunks.push((doc, score));
            if score > agg.best_score || (score == agg.best_score && doc < agg.best_id) {
                if score > agg.best_score && score > self.kth {
                    dirty = true;
                }
                agg.best_score = score;
                agg.best_id = doc;
            }
        }
        let full = self.is_full(k);
        let mut raised = None;
        if full && dirty {
            let mut bests: Vec<f32> = self.parents.values().map(|a| a.best_score).collect();
            let idx = k as usize - 1;
            let new_kth = *bests.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1;
            if new_kth > self.kth {
                self.kth = new_kth;
                raised = Some(self.kth);
            }
        }
        Ok(super::stream_run::BatchUpdate {
            full,
            changed: dirty,
            kth: raised,
        })
    }

    pub(super) fn finish(self, k: u32) -> (Vec<ScoredHit>, Vec<ParentGroup>, f32) {
        let mut ranked: Vec<(u64, ParentAgg)> = self.parents.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.best_score
                .total_cmp(&a.1.best_score)
                .then_with(|| a.1.best_id.cmp(&b.1.best_id))
        });
        let chunk_floor = if ranked.len() >= k as usize {
            ranked[k as usize - 1].1.best_score.next_down()
        } else {
            f32::NEG_INFINITY
        };
        ranked.truncate(k as usize);
        let mut hits = Vec::with_capacity(ranked.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (parent, agg) in ranked {
            hits.push(ScoredHit {
                identity: None,
                vector_id: agg.best_id,
                score: agg.best_score,
                parent_id: parent,
                rerank_candidate: None,
            });
            let mut chunks: Vec<(u64, f32)> = agg
                .chunks
                .into_iter()
                .filter(|&(_, score)| score >= chunk_floor)
                .collect();
            chunks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            groups.push(ParentGroup {
                parent_id: parent,
                chunks: chunks
                    .into_iter()
                    .map(|(doc, score)| ScoredHit {
                        identity: None,
                        vector_id: doc,
                        score,
                        parent_id: parent,
                        rerank_candidate: None,
                    })
                    .collect(),
            });
        }
        (hits, groups, chunk_floor)
    }
}
