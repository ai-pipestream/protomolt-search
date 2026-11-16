//! Decomposed run state, lexical leg loading and vector seeding.

use super::*;

pub(super) struct IdentitySession {
    pub(super) fanout: StreamFanout,
    pub(super) terminal: Vec<Option<StreamSearchSummary>>,
    pub(super) limits: crate::pb::StreamIdentityLimits,
}

pub(super) struct DecomposedRun<'a> {
    pub(super) round: HybridRound<'a>,
    pub(super) mask: Option<crate::placement::ShardMask>,
    pub(super) known: FilterKnown,
    pub(super) bm25: HashMap<u64, (f32, u32)>,
    pub(super) bm25_ranks: HashMap<u64, u32>,
    pub(super) lexical_counts: HashMap<u32, u32>,
    pub(super) lexical_top: Vec<(u64, f32, u32)>,
    pub(super) candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
    pub(super) vectors: HashMap<u64, (f32, u32)>,
    pub(super) bounds: LowerBounds,
    pub(super) filled: bool,
    pub(super) outside_bm25: f64,
    pub(super) summaries: Vec<Option<StreamSearchSummary>>,
    pub(super) clustered_counts: Vec<u32>,
    pub(super) identity: Option<IdentitySession>,
}

impl<'a> DecomposedRun<'a> {
    pub(super) fn new(coordinator: &CoordinatorServiceImpl, round: HybridRound<'a>) -> Self {
        let mask = coordinator.vector_scan_mask(round.query().input().filters().tree.as_ref());
        let known =
            CoordinatorServiceImpl::filter_known(round.query().input().filters(), mask.as_ref());
        let shards = coordinator.node_addrs.len();
        Self {
            round,
            mask,
            known,
            bm25: HashMap::new(),
            bm25_ranks: HashMap::new(),
            lexical_counts: HashMap::new(),
            lexical_top: Vec::new(),
            candidates: HashMap::new(),
            vectors: HashMap::new(),
            bounds: LowerBounds::new(round),
            filled: false,
            outside_bm25: 0.0,
            summaries: vec![None; shards],
            clustered_counts: vec![0; shards],
            identity: None,
        }
    }

    pub(super) fn spawn_lexical(
        &self,
        coordinator: &CoordinatorServiceImpl,
        admission: &Option<Arc<crate::vector_read::VectorReadBarrier>>,
    ) -> Result<Vec<LexicalTask>, Status> {
        let mut tasks = Vec::with_capacity(coordinator.node_addrs.len());
        let filters = self.round.query().input().filters();
        for (shard, node) in coordinator.node_addrs.iter().enumerate() {
            if self.mask.as_ref().is_some_and(|mask| mask.skipped[shard]) {
                continue;
            }
            let request = self.round.shard_request(
                coordinator.bm25_params,
                shard,
                admission
                    .as_ref()
                    .map(|barrier| barrier.context(shard))
                    .transpose()?,
                CoordinatorServiceImpl::shard_filter_tree(filters, self.mask.as_ref(), shard),
                true,
                true,
            );
            let mut client = coordinator.node_client(node)?;
            tasks.push(tokio::spawn(async move {
                client
                    .shard_legs(request)
                    .await
                    .map(|response| (shard as u32, response.into_inner()))
            }));
        }
        Ok(tasks)
    }

    pub(super) async fn load_lexical(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<(), Status> {
        if !self.round.terms().is_empty() {
            let admission = coordinator.vector_read_barrier()?;
            for task in self.spawn_lexical(coordinator, &admission)? {
                let (shard, response) = task.await.map_err(|error| {
                    Status::internal(format!("bm25 leg task failed: {error}"))
                })??;
                accept_hybrid_receipt(
                    &admission,
                    shard,
                    response.read_receipt.as_ref(),
                    "decomposed leg read receipt mismatch",
                )?;
                self.known
                    .merge(&response.geo_columns_known, &response.filter_columns_known)?;
                self.lexical_counts
                    .insert(shard, response.bm25_hits.len() as u32);
                for hit in response.bm25_hits {
                    self.bm25.insert(hit.doc_id, (hit.score, shard));
                    self.lexical_top.push((hit.doc_id, hit.score, shard));
                    if let Some(candidate) = hit.rerank_candidate {
                        self.candidates.entry(hit.doc_id).or_insert(candidate);
                    }
                }
            }
            if let Some(admission) = admission {
                admission.wait().await?;
            }
            self.lexical_top.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            self.lexical_top
                .truncate(self.round.query().legs().leg_k as usize);
        }
        let maximum = self
            .lexical_top
            .first()
            .map_or(0.0f32, |&(_, score, _)| score);
        self.bounds.weighted_bm25_max = self.bounds.bm25_weight * f64::from(maximum);
        self.filled = self.lexical_top.len() == self.round.query().legs().leg_k as usize
            && !self.lexical_top.is_empty();
        self.outside_bm25 = if self.filled {
            f64::from(self.lexical_top.last().expect("filled leg is non-empty").1)
        } else {
            0.0
        };
        self.bm25_ranks = self
            .lexical_top
            .iter()
            .enumerate()
            .map(|(rank, &(doc, _, _))| (doc, rank as u32 + 1))
            .collect();
        Ok(())
    }

    pub(super) async fn seed_vectors(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<Option<f32>, Status> {
        let mut ids: HashMap<u32, Vec<u64>> = HashMap::new();
        for &(doc, _, shard) in self.lexical_top.iter().take(self.bounds.k) {
            ids.entry(shard).or_default().push(doc);
        }
        let scores = coordinator
            .fanout_vector_rescore(
                self.round.query().input().vector(),
                ids,
                coordinator.vector_read_field.as_deref().unwrap_or(""),
            )
            .await?;
        for (&doc, &vector) in &scores {
            let (bm25, shard) = self.bm25[&doc];
            self.vectors.insert(doc, (vector, shard));
            self.bounds.push(vector, bm25);
        }
        Ok(self.bounds.initial_floor())
    }
}
