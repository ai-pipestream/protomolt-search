//! Exact weighted hybrid search with conservative vector floors and certified close-out.

use super::hybrid::accept_hybrid_receipt;
use super::hybrid_context::HybridRound;
use super::*;

/// Total-order f64 wrapper for heap keys.
#[derive(PartialEq)]
pub(super) struct F64Ord(pub(super) f64);

impl Eq for F64Ord {}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Magnitude-relative slack for the decomposed-floor bounds: 2^-40 of
/// the participating magnitudes, about four thousand times the worst
/// accumulated f64 rounding error of the three operations involved and
/// one part in 10^12 of the scores — provably conservative, immeasurably
/// loose.
pub(super) const SLACK_REL: f64 = 1.0 / (1u64 << 40) as f64;

/// The decomposed vector floor for a fused lower bound `s_lb`:
/// mathematically (s_lb - w_b * b_1) / w_v, rounded DOWN past every
/// f64 rounding error and the f32 cast, so that v(d) < result implies
/// w_v * v(d) + w_b * b(d) < s_lb for every doc (b(d) <= b_1 by the
/// exactness of the BM25 leg). `wb_b1` arrives premultiplied.
pub(super) fn decomposed_floor(s_lb: f64, wb_b1: f64, w_v: f64) -> f32 {
    let t = (s_lb - wb_b1) / w_v;
    let mag = t.abs() + (s_lb.abs() + wb_b1.abs()) / w_v;
    let safe = t - mag * SLACK_REL - f64::MIN_POSITIVE;
    (safe as f32).next_down()
}

type ExactCandidate = (u64, u32, f32, f32);
type VectorCandidate = (u64, u32, f32);
type RankedCandidate = (u64, u32, f32, f32, f64);
type LexicalTask = tokio::task::JoinHandle<Result<(u32, crate::pb::ShardLegsResponse), Status>>;

struct LowerBounds {
    k: usize,
    vector_weight: f64,
    bm25_weight: f64,
    weighted_bm25_max: f64,
    minimum_vector: Option<f32>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>>,
    lower: Option<f64>,
    #[cfg(feature = "net")]
    last_floor: f32,
}

impl LowerBounds {
    fn new(round: HybridRound<'_>) -> Self {
        let k = round.query().input().k() as usize;
        let legs = round.query().legs();
        Self {
            k,
            vector_weight: f64::from(legs.vector_weight),
            bm25_weight: f64::from(legs.bm25_weight),
            weighted_bm25_max: 0.0,
            minimum_vector: (legs.min_vector_score > 0.0).then_some(legs.min_vector_score),
            heap: std::collections::BinaryHeap::with_capacity(k + 1),
            lower: None,
            #[cfg(feature = "net")]
            last_floor: f32::NEG_INFINITY,
        }
    }

    fn fused(&self, vector: f32, bm25: f32) -> f64 {
        self.vector_weight * f64::from(vector) + self.bm25_weight * f64::from(bm25)
    }

    fn push(&mut self, vector: f32, bm25: f32) {
        if self.minimum_vector.is_some_and(|minimum| vector < minimum) {
            return;
        }
        let value = self.fused(vector, bm25);
        if self.heap.len() < self.k {
            self.heap.push(std::cmp::Reverse(F64Ord(value)));
        } else if self.heap.peek().is_some_and(|bound| value > bound.0 .0) {
            self.heap.pop();
            self.heap.push(std::cmp::Reverse(F64Ord(value)));
        }
    }

    fn initial_floor(&mut self) -> Option<f32> {
        self.lower = (self.heap.len() == self.k).then(|| self.heap.peek().expect("full heap").0 .0);
        let floor = self
            .lower
            .map(|lower| decomposed_floor(lower, self.weighted_bm25_max, self.vector_weight));
        let initial = match (floor, self.minimum_vector) {
            (Some(floor), Some(minimum)) => Some(floor.max(minimum)),
            (Some(floor), None) => Some(floor),
            (None, Some(minimum)) => Some(minimum),
            (None, None) => None,
        };
        #[cfg(feature = "net")]
        {
            self.last_floor = initial.unwrap_or(f32::NEG_INFINITY);
        }
        initial
    }

    #[cfg(feature = "net")]
    fn raised_floor(&mut self) -> Option<f32> {
        if self.heap.len() == self.k {
            let lower = self.heap.peek().expect("full heap").0 .0;
            if self.lower.is_none_or(|previous| lower > previous) {
                self.lower = Some(lower);
                let floor = decomposed_floor(lower, self.weighted_bm25_max, self.vector_weight);
                if floor > self.last_floor {
                    self.last_floor = floor;
                    return Some(floor);
                }
            }
        }
        None
    }
}

struct IdentitySession {
    fanout: StreamFanout,
    terminal: Vec<Option<StreamSearchSummary>>,
    limits: crate::pb::StreamIdentityLimits,
}

struct DecomposedRun<'a> {
    round: HybridRound<'a>,
    mask: Option<crate::placement::ShardMask>,
    known: FilterKnown,
    bm25: HashMap<u64, (f32, u32)>,
    bm25_ranks: HashMap<u64, u32>,
    lexical_counts: HashMap<u32, u32>,
    lexical_top: Vec<(u64, f32, u32)>,
    candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
    vectors: HashMap<u64, (f32, u32)>,
    bounds: LowerBounds,
    filled: bool,
    outside_bm25: f64,
    summaries: Vec<Option<StreamSearchSummary>>,
    clustered_counts: Vec<u32>,
    identity: Option<IdentitySession>,
}

impl CoordinatorServiceImpl {
    pub(super) async fn fanout_hybrid_decomposed(
        &self,
        round: HybridRound<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let mut run = DecomposedRun::new(self, round);
        let legs_started = std::time::Instant::now();
        run.load_lexical(self).await?;
        let initial_floor = run.seed_vectors(self).await?;
        #[cfg(feature = "net")]
        run.scan_vectors(self, initial_floor).await?;
        #[cfg(not(feature = "net"))]
        let _ = initial_floor;
        let legs_ms = legs_started.elapsed().as_secs_f32() * 1e3;
        let fusion_started = std::time::Instant::now();
        let hits = run.close_out(self).await?;
        let debug = run.debug(legs_ms, fusion_started);
        Ok((hits, debug))
    }
}

impl<'a> DecomposedRun<'a> {
    fn new(coordinator: &CoordinatorServiceImpl, round: HybridRound<'a>) -> Self {
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

    fn spawn_lexical(
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

    async fn load_lexical(&mut self, coordinator: &CoordinatorServiceImpl) -> Result<(), Status> {
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

    async fn seed_vectors(
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

    #[cfg(feature = "net")]
    fn observe_vector(&mut self, doc: u64, vector: f32, shard: u32) {
        if self.vectors.contains_key(&doc) {
            return;
        }
        self.vectors.insert(doc, (vector, shard));
        let bm25 = self.bm25.get(&doc).map_or(0.0, |&(score, _)| score);
        self.bounds.push(vector, bm25);
    }

    #[cfg(feature = "net")]
    async fn scan_vectors(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        if coordinator.clustered_vectors.is_some() {
            self.scan_clustered(coordinator, initial_floor).await
        } else {
            self.scan_plain(coordinator, initial_floor).await
        }
    }

    #[cfg(feature = "net")]
    async fn scan_clustered(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        let input = self.round.query().input();
        let backend = coordinator
            .clustered_vectors
            .as_ref()
            .expect("clustered route selected");
        let ranges = coordinator.product_label_ranges().await?;
        let allowed = coordinator
            .clustered_allowed_labels(input.filters())
            .await?;
        let mut stream = backend
            .candidate_stream(
                input.request_id(),
                input.vector().to_vec(),
                allowed,
                initial_floor,
            )
            .await?;
        let mut labels = std::collections::HashSet::new();
        let completion = loop {
            match stream.next_event().await? {
                ClusteredCandidateEvent::Batch(batch) => {
                    for candidate in batch {
                        if !candidate.score.is_finite() {
                            return Err(Status::internal(format!(
                                "clustered TurboVec label {} has non-finite score {}",
                                candidate.label, candidate.score
                            )));
                        }
                        if !labels.insert(candidate.label) {
                            return Err(Status::failed_precondition(format!(
                                "clustered TurboVec emitted duplicate stable label {}",
                                candidate.label
                            )));
                        }
                        let shard =
                            CoordinatorServiceImpl::product_owner(&ranges, candidate.label)?;
                        self.clustered_counts[shard as usize] =
                            self.clustered_counts[shard as usize].saturating_add(1);
                        self.observe_vector(candidate.label, candidate.score, shard);
                    }
                    if let Some(floor) = self.bounds.raised_floor() {
                        stream.raise_floor(floor)?;
                    }
                }
                ClusteredCandidateEvent::Completion(completion) => break completion,
            }
        };
        if completion.emitted != labels.len() as u64 {
            return Err(Status::internal(format!("clustered TurboVec completion counted {} candidates but decomposed search received {}", completion.emitted, labels.len())));
        }
        Ok(())
    }

    #[cfg(feature = "net")]
    async fn scan_plain(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        initial_floor: Option<f32>,
    ) -> Result<(), Status> {
        coordinator.ensure_shard_vector_rows().await;
        let input = self.round.query().input();
        let limits =
            input
                .filters()
                .include_rerank_candidate
                .then_some(crate::pb::StreamIdentityLimits {
                    max_rows: input.k(),
                    max_response_bytes: 32 * 1024 * 1024,
                    timeout_ms: 60_000,
                });
        let mut fanout = coordinator.open_stream_fanout_with_identities(
            input.request_id(),
            input.vector(),
            initial_floor,
            Some(input.k()),
            false,
            input.filters(),
            limits,
        )?;
        let mut remaining = self.summaries.len();
        for (shard, summary) in self.summaries.iter_mut().enumerate() {
            if self.mask.as_ref().is_some_and(|mask| mask.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        let terminal = self.summaries.clone();
        while remaining > 0 {
            let (shard, message) = match fanout.next_message(&self.summaries).await {
                Ok(Some(pair)) => pair,
                Ok(None) => continue,
                Err(error) => return fanout.cancel_with(error).await,
            };
            match self.accept_stream_message(
                coordinator,
                &mut fanout,
                shard,
                message,
                limits.is_some(),
            ) {
                Ok(true) => remaining -= 1,
                Ok(false) => {}
                Err(error) => return fanout.cancel_with(error).await,
            }
        }
        self.known.refuse_unknown(input.filters())?;
        if let Some(limits) = limits {
            self.identity = Some(IdentitySession {
                fanout,
                terminal,
                limits,
            });
        }
        Ok(())
    }

    #[cfg(feature = "net")]
    fn accept_stream_message(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
        fanout: &mut StreamFanout,
        shard: usize,
        message: StreamSearchResponse,
        identities: bool,
    ) -> Result<bool, Status> {
        let summary = match message.payload {
            Some(stream_search_response::Payload::Batch(batch)) => {
                if batch.hits.len() % 12 != 0 {
                    return Err(Status::internal(format!(
                        "shard {shard} sent a misaligned batch of {} bytes",
                        batch.hits.len()
                    )));
                }
                for record in batch.hits.as_chunks::<12>().0 {
                    let doc = u64::from_le_bytes(record[..8].try_into().expect("8-byte id"));
                    let vector =
                        f32::from_le_bytes(record[8..12].try_into().expect("4-byte score"));
                    self.observe_vector(doc, vector, shard as u32);
                }
                if let Some(floor) = self.bounds.raised_floor() {
                    coordinator.push_stream_floor(fanout, floor);
                }
                return Ok(false);
            }
            Some(stream_search_response::Payload::IdentityReady(ready)) if identities => ready
                .scan
                .ok_or_else(|| Status::internal("IdentityReady has no scan certificate"))?,
            Some(stream_search_response::Payload::Summary(summary)) if !identities => summary,
            Some(_) => return Err(Status::internal("unexpected identity exchange on stream")),
            None => return Ok(false),
        };
        if !summary.completed {
            return Err(Status::internal(format!(
                "shard {shard} stopped before completing its scan"
            )));
        }
        self.known.merge_shard(
            shard,
            &summary.geo_columns_known,
            &summary.filter_columns_known,
        )?;
        self.summaries[shard] = Some(summary);
        if !identities {
            fanout.mark_completed(shard);
        }
        Ok(true)
    }

    fn exact_and_unknown(&self) -> (Vec<ExactCandidate>, Vec<VectorCandidate>) {
        let mut exact = Vec::new();
        let mut unknown = Vec::new();
        for (&doc, &(vector, shard)) in &self.vectors {
            if self
                .bounds
                .minimum_vector
                .is_some_and(|minimum| vector < minimum)
            {
                continue;
            }
            match self.bm25.get(&doc) {
                Some(&(bm25, _)) => exact.push((doc, shard, vector, bm25)),
                None if !self.filled => exact.push((doc, shard, vector, 0.0)),
                None => unknown.push((doc, shard, vector)),
            }
        }
        (exact, unknown)
    }

    fn rescore_candidates(
        &self,
        exact: &[ExactCandidate],
        unknown: Vec<VectorCandidate>,
    ) -> (HashMap<u32, Vec<u64>>, Vec<VectorCandidate>) {
        let mut fused: Vec<f64> = exact
            .iter()
            .map(|&(_, _, vector, bm25)| self.bounds.fused(vector, bm25))
            .collect();
        let known = if fused.len() >= self.bounds.k {
            *fused
                .select_nth_unstable_by(self.bounds.k - 1, |a, b| b.total_cmp(a))
                .1
        } else {
            f64::NEG_INFINITY
        };
        let mut ids: HashMap<u32, Vec<u64>> = HashMap::new();
        let mut docs = Vec::new();
        for (doc, shard, vector) in unknown {
            let upper = self.bounds.vector_weight * f64::from(vector)
                + self.bounds.bm25_weight * self.outside_bm25;
            let magnitude = upper.abs()
                + (self.bounds.vector_weight * f64::from(vector)).abs()
                + self.bounds.bm25_weight * self.outside_bm25;
            let safe_upper = upper + magnitude * SLACK_REL + f64::MIN_POSITIVE;
            if safe_upper >= known {
                ids.entry(shard).or_default().push(doc);
                docs.push((doc, shard, vector));
            }
        }
        (ids, docs)
    }

    async fn close_out(
        &mut self,
        coordinator: &CoordinatorServiceImpl,
    ) -> Result<Vec<HybridHit>, Status> {
        let (mut exact, unknown) = self.exact_and_unknown();
        let (ids, docs) = self.rescore_candidates(&exact, unknown);
        let rescored = coordinator
            .fanout_bm25_rescore_scores(
                self.round.terms(),
                self.round.analysis_fingerprint(),
                self.round.global(),
                self.round.claims(),
                ids,
                &[],
            )
            .await?;
        for (doc, shard, vector) in docs {
            exact.push((
                doc,
                shard,
                vector,
                rescored.get(&doc).copied().unwrap_or(0.0),
            ));
        }
        let mut ranked: Vec<RankedCandidate> = exact
            .into_iter()
            .map(|(doc, shard, vector, bm25)| {
                (doc, shard, vector, bm25, self.bounds.fused(vector, bm25))
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.4.total_cmp(&a.4)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(self.bounds.k);
        self.resolve_candidates(&ranked).await?;
        Ok(ranked
            .into_iter()
            .map(|(doc, shard, vector, bm25, fused)| HybridHit {
                doc_id: doc,
                fused_score: fused as f32,
                shard,
                vector_rank: None,
                vector_score: vector,
                bm25_rank: self.bm25_ranks.get(&doc).copied(),
                bm25_score: bm25,
                boost_score: 0.0,
                vector_normalized: None,
                bm25_normalized: None,
                rerank_candidate: self.candidates.remove(&doc),
            })
            .collect())
    }

    async fn resolve_candidates(&mut self, ranked: &[RankedCandidate]) -> Result<(), Status> {
        if let Some(mut session) = self.identity.take() {
            let hits: Vec<MergedHit> = ranked
                .iter()
                .filter(|(doc, _, _, _, _)| !self.candidates.contains_key(doc))
                .map(|&(doc, shard, vector, _, _)| MergedHit {
                    vector_id: doc,
                    shard,
                    score: vector,
                })
                .collect();
            let (_, candidates) = session
                .fanout
                .resolve_identities_and_candidates(
                    &hits,
                    &self.summaries,
                    &mut session.terminal,
                    &session.limits,
                )
                .await?;
            for ((_, doc), candidate) in candidates {
                if let Some(candidate) = candidate {
                    self.candidates.entry(doc).or_insert(candidate);
                }
            }
        }
        Ok(())
    }

    fn debug(&self, legs_ms: f32, fusion_started: std::time::Instant) -> Option<HybridDebug> {
        self.round.query().debug().then(|| {
            let shards = self
                .summaries
                .iter()
                .enumerate()
                .map(|(shard, summary)| HybridShardDebug {
                    shard: shard as u32,
                    rpc_ms: 0.0,
                    vector_hits: summary
                        .as_ref()
                        .map_or(self.clustered_counts[shard], |summary| {
                            u32::try_from(summary.emitted).unwrap_or(u32::MAX)
                        }),
                    bm25_hits: self
                        .lexical_counts
                        .get(&(shard as u32))
                        .copied()
                        .unwrap_or(0),
                    scan: None,
                })
                .collect();
            HybridDebug {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: self.round.query().legs().leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: fusion_started.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        })
    }
}

#[cfg(all(test, feature = "net"))]
#[path = "hybrid_decomposed_tests.rs"]
mod tests;
