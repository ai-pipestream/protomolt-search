//! Shared flat and fused BM25 fan-out and certified winner reduction.

use super::*;

struct PreparedRound<'a> {
    request: Bm25QueryRequest,
    claims: &'a [StatsClaim],
    phrase: Option<(usize, &'a [f32], u64)>,
    flat: bool,
    timing: Option<FusedTiming>,
}

#[derive(Clone, Copy)]
struct FusedTiming {
    timing: RoundTiming,
    fields: usize,
    terms: usize,
}
pub(super) struct Bm25Reply {
    pub(super) shard: u32,
    pub(super) response: Bm25QueryResponse,
    pub(super) fingerprint: Option<String>,
    pub(super) millis: f64,
}

impl CoordinatorServiceImpl {
    pub(super) async fn bm25_query_round(
        &self,
        round: FlatRound<'_>,
    ) -> Result<AggregatedHits, Status> {
        let FlatRound {
            query,
            terms,
            analysis_fingerprint,
            global,
            claims,
        } = round;
        let input = query.input();
        let score_stages = &input.score_stages;
        let stats_fields = &input.stats_fields;
        let cardinality_fields = &input.cardinality_fields;
        let projections = query.projections();
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let request = Bm25QueryRequest {
            analysis_fingerprint,
            terms: terms.to_vec(),
            global_doc_count: global.doc_count,
            global_total_doc_length: global.total_doc_length,
            global_doc_frequencies: global.dfs.clone(),
            k1: self.bm25_params.k1 as f32,
            b: self.bm25_params.b as f32,
            projections: projections.to_vec(),
            score_stages: score_stages.to_vec(),
            stats_fields: stats_fields.to_vec(),
            cardinality_fields: cardinality_fields.to_vec(),
            ..query.options().request()
        };
        self.execute_bm25_round(PreparedRound {
            request,
            claims,
            phrase: None,
            flat: true,
            timing: None,
        })
        .await
    }
    pub(super) async fn bm25_fused_round(
        &self,
        round: FusedRound<'_>,
    ) -> Result<(FacetedHits, crate::segment_prune::PruneStats), Status> {
        let FusedRound {
            query,
            fields,
            field_terms,
            globals,
            claims,
            phrase,
            phrase_legs,
            fingerprints,
            timing,
        } = round;
        let k = query.k();
        let min_score = query.request().min_score;
        let trace = timing.trace;

        let doc_count = globals.doc_count;
        let totals = &globals.totals;
        let dfs = &globals.dfs;
        // (c) Bm25Query fan-out with per-field legs in entry order.
        // Entry k1/b of 0 pick up the coordinator's configured params,
        // so tuning reaches this path too.
        let legs: Vec<crate::pb::Bm25FieldLeg> = fields
            .iter()
            .enumerate()
            .map(|(fi, f)| crate::pb::Bm25FieldLeg {
                field: f.field.clone(),
                terms: field_terms[fi].clone(),
                global_total_doc_length: totals[fi],
                global_doc_frequencies: dfs[fi].clone(),
                weight: f.weight,
                k1: if f.k1 == 0.0 {
                    self.bm25_params.k1 as f32
                } else {
                    f.k1
                },
                b: if f.b == 0.0 {
                    self.bm25_params.b as f32
                } else {
                    f.b
                },
                // Declare which analyzer produced these terms so the
                // shard can refuse a column built under a different one.
                // The caller computed it from THE SPEC ACTUALLY USED,
                // not from what the caller meant.
                analysis_fingerprint: fingerprints[fi],
                phrase: phrase_legs[fi].clone(),
            })
            .collect();
        debug_assert_eq!(phrase_legs.len(), fields.len());
        debug_assert_eq!(fingerprints.len(), fields.len());
        let _ = phrase.map(|(leg, _, _)| leg);
        if trace {
            for l in &legs {
                eprintln!(
                    "bm25-fused leg: field={:?} terms={:?} dfs={:?} total_len={} w={} k1={} b={} \
                     | req k={k} N={doc_count} min_score={min_score}",
                    l.field,
                    l.terms,
                    l.global_doc_frequencies,
                    l.global_total_doc_length,
                    l.weight,
                    l.k1,
                    l.b
                );
            }
        }
        let request = Bm25QueryRequest {
            global_doc_count: doc_count,
            fields: legs,
            ..query.request()
        };
        let result = self
            .execute_bm25_round(PreparedRound {
                request,
                claims,
                phrase,
                flat: false,
                timing: Some(FusedTiming {
                    timing,
                    fields: fields.len(),
                    terms: field_terms.iter().map(Vec::len).sum(),
                }),
            })
            .await?;
        Ok(((result.0, result.1, result.2), result.5))
    }
    async fn execute_bm25_round(&self, round: PreparedRound<'_>) -> Result<AggregatedHits, Status> {
        let stream_enabled = self.bm25_stream && round.phrase.is_none();
        let relay = stream_enabled
            .then(|| watch::channel(round.request.min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = stream_enabled.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(round.request.k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        let mask = self.shard_mask(round.request.filter.as_ref());
        let mut tasks = self.spawn_bm25_round(&round, mask.as_ref(), relay, stream_heap.clone())?;
        let mut responses = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.shard);
        let mut merged = super::lexical_reduce::RoundMerge::new(&round.request, round.flat);
        for response in responses {
            merged.accept(response)?;
        }
        merged.validate_columns(mask.as_ref())?;
        let summaries = merged.summaries()?;
        self.publish_bm25_snapshot(stream_heap.as_ref(), merged.fingerprint());
        let t_query = round.timing.map(|t| t.timing.started.elapsed());
        if round.timing.is_some_and(|t| t.timing.trace) {
            merged.trace_shards();
        }
        let prune = merged.prune();
        let hits = finish_bm25_hits(merged.into_hits(), stream_heap, round.request.k)?;
        trace_fused_timing(round.timing, t_query);
        Ok((
            hits,
            summaries.0,
            summaries.1,
            summaries.2,
            summaries.3,
            prune,
        ))
    }

    fn spawn_bm25_round(
        &self,
        round: &PreparedRound<'_>,
        mask: Option<&crate::placement::ShardMask>,
        relay: Option<FloorRelay>,
        heap: Option<Arc<Mutex<Bm25StreamHeap>>>,
    ) -> Result<tokio::task::JoinSet<Result<Bm25Reply, Status>>, Status> {
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let mut request = round.request.clone();
            request.expected_stats_epoch = round.claims[shard].epoch;
            request.expected_stats_incarnation = round.claims[shard].incarnation();
            let phrase = round
                .phrase
                .map(|(leg, weights, _)| crate::pb::Bm25PhraseQueryRequest {
                    query: Some(request.clone()),
                    phrase_leg: leg as u32,
                    phrase_term_weights: weights.to_vec(),
                });
            let transport = ShardRound {
                shard: shard as u32,
                request,
                phrase,
                client: self.node_client(node)?,
                relay: relay.clone(),
                heap: heap.clone(),
                deadline: self.limits.shard_deadline,
            };
            tasks.spawn(transport.run());
        }
        Ok(tasks)
    }

    fn publish_bm25_snapshot(
        &self,
        heap: Option<&Arc<Mutex<Bm25StreamHeap>>>,
        fingerprint: Option<&String>,
    ) {
        if let (Some(heap), Some(fingerprint)) = (heap, fingerprint) {
            let snapshot = super::lexical_stream::lexical_snapshot(
                &heap.lock().expect("BM25 stream heap poisoned"),
            );
            super::lexical_stream::publish_lexical_progress(
                self.query_progress.as_deref(),
                snapshot,
                fingerprint.clone(),
            );
        }
    }
}

type FloorRelay = (Arc<watch::Sender<f32>>, watch::Receiver<f32>);
struct ShardRound {
    shard: u32,
    request: Bm25QueryRequest,
    phrase: Option<crate::pb::Bm25PhraseQueryRequest>,
    client: crate::link::NodeLink,
    relay: Option<FloorRelay>,
    heap: Option<Arc<Mutex<Bm25StreamHeap>>>,
    deadline: Option<Duration>,
}

impl ShardRound {
    async fn run(mut self) -> Result<Bm25Reply, Status> {
        let started = std::time::Instant::now();
        let (response, fingerprint) = if let Some((tx, rx)) = self.relay {
            let run = stream_bm25_shard(
                self.shard,
                self.request.k as usize,
                self.client,
                self.request,
                tx,
                rx,
                self.heap.expect("relay has heap"),
            );
            let result = match self.deadline {
                Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                    Status::deadline_exceeded(format!(
                        "BM25 shard {} exceeded its {}ms deadline",
                        self.shard,
                        limit.as_millis()
                    ))
                })?,
                None => run.await,
            }?;
            (result.response, Some(result.scoring_fingerprint))
        } else {
            let response = match self.phrase {
                Some(request) => self.client.bm25_phrase_query(request).await,
                None => self.client.bm25_query(self.request).await,
            }?;
            (response.into_inner(), None)
        };
        Ok(Bm25Reply {
            shard: self.shard,
            response,
            fingerprint,
            millis: started.elapsed().as_secs_f64() * 1000.0,
        })
    }
}

fn trace_fused_timing(timing: Option<FusedTiming>, query_elapsed: Option<Duration>) {
    if let Some(t) = timing.filter(|t| t.timing.trace) {
        let t_query = query_elapsed.expect("fused round has timing");
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        eprintln!("bm25-fused: analyze {:.1} ms, term-stats {:.1} ms, query fan-out {:.1} ms, merge {:.1} ms  ({} fields, {} terms)", ms(t.timing.analyzed), ms(t.timing.stats - t.timing.analyzed), ms(t_query - t.timing.stats), ms(t.timing.started.elapsed() - t_query), t.fields, t.terms);
    }
}

pub(super) fn finish_bm25_hits(
    mut all: Vec<(u32, Bm25Hit)>,
    stream_heap: Option<Arc<Mutex<Bm25StreamHeap>>>,
    k: u32,
) -> Result<Vec<Bm25Hit>, Status> {
    if let Some(stream_heap) = stream_heap {
        let winners: Vec<MergedHit> = {
            let state = stream_heap.lock().expect("BM25 stream heap poisoned");
            let mut winners: Vec<MergedHit> = state.heap.iter().map(|entry| entry.0).collect();
            winners.sort_by(crate::merge::cmp_lexical_hits);
            winners
        };
        let mut details: HashMap<(u32, u64), Bm25Hit> = all
            .drain(..)
            .map(|(shard, hit)| ((shard, hit.doc_id), hit))
            .collect();
        all = winners
                .into_iter()
                .map(|winner| {
                    let hit = details
                        .remove(&(winner.shard, winner.vector_id))
                        .ok_or_else(|| {
                            Status::data_loss(format!(
                                "BM25 global winner {} from shard {} was absent from its certified local top-k",
                                winner.vector_id, winner.shard
                            ))
                        })?;
                    if hit.score.to_bits() != winner.score.to_bits() {
                        return Err(Status::data_loss(format!(
                            "BM25 global winner {} from shard {} changed score from {:?} in the candidate stream to {:?} in the certified response",
                            winner.vector_id,
                            winner.shard,
                            winner.score,
                            hit.score
                        )));
                    }
                    Ok((winner.shard, hit))
                })
                .collect::<Result<_, _>>()?;
    } else {
        all.sort_by(|(sa, a), (sb, b)| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| sa.cmp(sb))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        all.truncate(k as usize);
    }
    Ok(all.into_iter().map(|(_, hit)| hit).collect())
}
