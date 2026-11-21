//! Validated hybrid execution with shared shard dispatch and ranked fusion.

mod dispatch;

use self::dispatch::LocalHybridReply;
use super::hybrid_context::{HybridInput, HybridQuery, HybridRound};
use super::hybrid_reduce::{finish_two_level, fuse_clustered_shard, hybrid_debug, GlobalHybrid};
use super::*;

impl CoordinatorServiceImpl {
    /// Hybrid vector + BM25 search:
    ///
    /// 1. analyze `text` into query terms (same analysis options as
    ///    ingest, as in [`Self::fanout_bm25`]);
    /// 2. TermStats fan-out and merge into GLOBAL corpus stats;
    /// 3. fuse per `legs.fusion_mode`: GLOBAL_RANK and SCORE_BLEND fetch
    ///    raw shard legs and fuse once on the coordinator (RRF over
    ///    global ranks, or normalize-and-combine over global scores);
    ///    TWO_LEVEL lets each shard RRF-fuse locally and RRF-merges the
    ///    shard lists (the fallback for incomparable scores).
    pub async fn fanout_hybrid_context(
        &self,
        query: HybridQuery<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let (hits, debug, _) = self.fanout_hybrid_with_terms(query).await?;
        Ok((hits, debug))
    }

    pub(super) async fn fanout_hybrid_with_terms(
        &self,
        query: HybridQuery<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>, DenseScanExecution), Status> {
        let input = query.input();
        self.check_vector_scan(input.filters(), false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_hybrid_with_terms(query)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        if input.k() == 0 || input.vector().is_empty() {
            return Ok((Vec::new(), None, DenseScanExecution::default()));
        }
        if !self.hybrid_is_clustered() && query.legs().vector_weight > 0.0 {
            self.ensure_shard_vector_rows().await;
        }
        let total_started = std::time::Instant::now();
        let (terms, analysis_ms) = self.analyze_hybrid_input(input, true).await?;
        let (hits, mut debug, stats_ms, exec) = self.hybrid_stats_rounds(query, &terms).await?;
        if let Some(debug) = debug.as_mut() {
            debug.terms = terms;
            debug.analysis_ms = analysis_ms;
            debug.stats_ms = stats_ms;
            debug.total_ms = total_started.elapsed().as_secs_f32() * 1e3;
        }
        Ok((hits, debug, exec))
    }

    pub(super) async fn analyze_hybrid_input(
        &self,
        input: HybridInput<'_>,
        omit_empty: bool,
    ) -> Result<(Vec<String>, f32), Status> {
        let mut terms = Vec::new();
        if !input.pre_analyzed_terms().is_empty() {
            for term in input.pre_analyzed_terms() {
                if !(terms.contains(term) || omit_empty && term.is_empty()) {
                    terms.push(term.clone());
                }
            }
            return Ok((terms, 0.0));
        }
        let address = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let started = std::time::Instant::now();
        let analyzed =
            crate::analyzer::analyze_document(&address, input.text(), input.spec()).await?;
        let ms = started.elapsed().as_secs_f32() * 1e3;
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        Ok((terms, ms))
    }

    async fn hybrid_stats_rounds(
        &self,
        query: HybridQuery<'_>,
        terms: &[String],
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>, f32, DenseScanExecution), Status> {
        let started = std::time::Instant::now();
        let fingerprint = crate::analyzer::analysis_fingerprint(query.input().spec());
        let mut fresh = false;
        loop {
            let (global, claims) = self.body_stats(terms, fresh).await?;
            let stats_ms = started.elapsed().as_secs_f32() * 1e3;
            let round = HybridRound::new(
                query,
                terms,
                fingerprint,
                &global,
                &claims,
                self.node_addrs.len(),
            )?;
            let result = match query.legs().fusion_mode {
                FusionMode::TwoLevel => self.fanout_hybrid_two_level(round).await,
                FusionMode::Decomposed => self.fanout_hybrid_decomposed(round).await,
                _ => self.fanout_hybrid_global_rank(round).await,
            };
            match result {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(error) => return Err(error),
                Ok((hits, debug, exec)) => return Ok((hits, debug, stats_ms, exec)),
            }
        }
    }
}

impl CoordinatorServiceImpl {
    fn hybrid_is_clustered(&self) -> bool {
        #[cfg(feature = "net")]
        {
            self.clustered_vectors.is_some()
        }
        #[cfg(not(feature = "net"))]
        {
            false
        }
    }

    async fn hybrid_clustered_vectors(
        &self,
        round: HybridRound<'_>,
        two_level: bool,
    ) -> Result<Vec<Vec<(u64, f64)>>, Status> {
        #[cfg(feature = "net")]
        {
            let ranges = self.product_label_ranges().await?;
            let query = round.query();
            if !two_level && query.legs().vector_weight == 0.0 {
                return Ok(vec![Vec::new(); self.node_addrs.len()]);
            }
            self.clustered_local_vector_legs(
                query.input().request_id(),
                query.input().vector(),
                query.legs().leg_k,
                query.input().filters(),
                &ranges,
            )
            .await
        }
        #[cfg(not(feature = "net"))]
        {
            let _ = (round, two_level);
            unreachable!("clustered vectors require the net feature")
        }
    }

    /// The dense read barrier and shard mask of one hybrid round; a clustered
    /// provider scans no product shard, so it takes no barrier.
    fn hybrid_admission(
        &self,
        query: HybridQuery<'_>,
        clustered: bool,
    ) -> Result<HybridAdmission, Status> {
        let admission = if clustered {
            None
        } else {
            self.vector_read_barrier()?
        };
        let mask = if clustered {
            self.shard_mask(query.input().filters().tree.as_ref())
        } else {
            self.vector_scan_mask(query.input().filters().tree.as_ref())
        };
        Ok((admission, mask))
    }

    /// Fold the clustered provider's per-shard vector legs into `merged`;
    /// returns each shard's vector hit count.
    async fn collect_clustered_legs(
        &self,
        round: HybridRound<'_>,
        merged: &mut GlobalHybrid,
    ) -> Result<Vec<u32>, Status> {
        let mut vector_counts = vec![0u32; self.node_addrs.len()];
        for (shard, hits) in self
            .hybrid_clustered_vectors(round, false)
            .await?
            .into_iter()
            .enumerate()
        {
            vector_counts[shard] = u32::try_from(hits.len()).unwrap_or(u32::MAX);
            for &(id, _) in &hits {
                merged.owners.insert(id, shard as u32);
            }
            merged.vector.push((shard as u32, hits));
        }
        Ok(vector_counts)
    }

    async fn hybrid_leg_execution(
        &self,
        clustered: bool,
        vector_weight: f32,
        scans: &[(usize, Option<ShardScanStats>)],
    ) -> Result<DenseScanExecution, Status> {
        if vector_weight == 0.0 {
            return Ok(DenseScanExecution::default());
        }
        if clustered {
            let rows = self
                .clustered_quality_identity()
                .await?
                .map_or(0, |id| id.rows);
            return Ok(clustered_scan_execution(rows));
        }
        let refs: Vec<(usize, Option<&ShardScanStats>)> =
            scans.iter().map(|(s, st)| (*s, st.as_ref())).collect();
        Ok(self.evidence_from_shard_stats(&refs, 0))
    }

    pub(super) async fn fanout_hybrid_global_rank(
        &self,
        round: HybridRound<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>, DenseScanExecution), Status> {
        let started = std::time::Instant::now();
        let query = round.query();
        let clustered = self.hybrid_is_clustered();
        let mut merged = GlobalHybrid::default();
        let vector_counts = if clustered {
            self.collect_clustered_legs(round, &mut merged).await?
        } else {
            vec![0u32; self.node_addrs.len()]
        };
        let (admission, mask) = self.hybrid_admission(query, clustered)?;
        let tasks = self.spawn_global_hybrid(round, mask.as_ref(), &admission, clustered)?;
        let mut known = Self::filter_known(query.input().filters(), mask.as_ref());
        let mut shard_debug = Vec::new();
        let mut scans = Vec::new();
        for task in tasks {
            let (shard, rpc_ms, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            if !clustered {
                accept_hybrid_receipt(
                    &admission,
                    shard,
                    response.read_receipt.as_ref(),
                    "hybrid leg read receipt mismatch",
                )?;
            }
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            if query.debug() {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: if clustered {
                        vector_counts[shard as usize]
                    } else {
                        response.vector_hits.len() as u32
                    },
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: response.vector_scan,
                });
            }
            if !clustered {
                scans.push((shard as usize, response.vector_scan));
                merged.collect_raw(shard, response.vector_hits, true);
            }
            merged.collect_raw(shard, response.bm25_hits, false);
        }
        if let Some(admission) = &admission {
            admission.wait().await?;
        }
        known.refuse_unknown(query.input().filters())?;
        let exec = self
            .hybrid_leg_execution(clustered, query.legs().vector_weight, &scans)
            .await?;
        let legs_ms = started.elapsed().as_secs_f32() * 1e3;
        let fusion_started = std::time::Instant::now();
        let hits = merged.finish(query);
        Ok((
            hits,
            hybrid_debug(query, shard_debug, legs_ms, fusion_started),
            exec,
        ))
    }

    pub(super) async fn fanout_hybrid_two_level(
        &self,
        round: HybridRound<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>, DenseScanExecution), Status> {
        let started = std::time::Instant::now();
        let query = round.query();
        let clustered = self.hybrid_is_clustered();
        let vector_legs = if clustered {
            self.hybrid_clustered_vectors(round, true).await?
        } else {
            Vec::new()
        };
        let (admission, mask) = self.hybrid_admission(query, clustered)?;
        let tasks =
            self.spawn_local_hybrid(round, mask.as_ref(), &admission, clustered, &vector_legs)?;
        let mut known = Self::filter_known(query.input().filters(), mask.as_ref());
        let mut shard_debug = Vec::new();
        let mut lists = Vec::new();
        let mut scans = Vec::new();
        let task_name = if clustered {
            "shard legs"
        } else {
            "hybrid shard"
        };
        for task in tasks {
            let (shard, rpc_ms, reply) = task
                .await
                .map_err(|error| Status::internal(format!("{task_name} task failed: {error}")))??;
            let (hits, scan) =
                unpack_two_level_reply(shard, reply, query, &admission, &mut known, &mut scans)?;
            if query.debug() {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: hits.iter().filter(|hit| hit.vector_rank.is_some()).count() as u32,
                    bm25_hits: hits.iter().filter(|hit| hit.bm25_rank.is_some()).count() as u32,
                    scan,
                });
            }
            lists.push((shard, hits));
        }
        if let Some(admission) = &admission {
            admission.wait().await?;
        }
        known.refuse_unknown(query.input().filters())?;
        let exec = self
            .hybrid_leg_execution(clustered, query.legs().vector_weight, &scans)
            .await?;
        let legs_ms = started.elapsed().as_secs_f32() * 1e3;
        let fusion_started = std::time::Instant::now();
        let hits = finish_two_level(lists, query);
        Ok((
            hits,
            hybrid_debug(query, shard_debug, legs_ms, fusion_started),
            exec,
        ))
    }
}

fn unpack_two_level_reply(
    shard: u32,
    reply: LocalHybridReply,
    query: HybridQuery<'_>,
    admission: &Option<Arc<crate::vector_read::VectorReadBarrier>>,
    known: &mut FilterKnown,
    scans: &mut Vec<(usize, Option<ShardScanStats>)>,
) -> Result<(Vec<crate::pb::HybridLegHit>, Option<ShardScanStats>), Status> {
    let (mut hits, scan) = match reply {
        LocalHybridReply::Clustered { response, vector } => {
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            (
                fuse_clustered_shard(vector, response.bm25_hits, query.legs()),
                None,
            )
        }
        LocalHybridReply::Plain(response) => {
            accept_hybrid_receipt(
                admission,
                shard,
                response.read_receipt.as_ref(),
                "hybrid fusion read receipt mismatch",
            )?;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            scans.push((shard as usize, response.vector_scan));
            (response.hits, response.vector_scan)
        }
    };
    if query.legs().min_vector_score > 0.0 {
        hits.retain(|hit| {
            hit.vector_rank.is_some() && hit.vector_score >= query.legs().min_vector_score
        });
    }
    Ok((hits, scan))
}

type HybridAdmission = (
    Option<Arc<crate::vector_read::VectorReadBarrier>>,
    Option<crate::placement::ShardMask>,
);

pub(super) fn accept_hybrid_receipt(
    admission: &Option<Arc<crate::vector_read::VectorReadBarrier>>,
    shard: u32,
    receipt: Option<&crate::pb::VectorReadReceipt>,
    mismatch: &str,
) -> Result<(), Status> {
    match (admission, receipt) {
        (Some(admission), Some(receipt)) => admission.accept(shard as usize, receipt),
        (None, None) => Ok(()),
        _ => Err(Status::failed_precondition(mismatch)),
    }
}
