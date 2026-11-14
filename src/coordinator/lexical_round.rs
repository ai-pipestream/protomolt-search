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
struct Bm25Reply {
    shard: u32,
    response: Bm25QueryResponse,
    fingerprint: Option<String>,
    millis: f64,
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
        let LexicalFanout {
            score_stages,
            stats_fields,
            cardinality_fields,
            projections,
            ..
        } = query.input();
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
            ..query.request()
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
        let LexicalFanout { k, min_score, .. } = query.input();
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
        let k = round.request.k;
        let min_score = round.request.min_score;
        let facet_fields = round.request.facet_fields.as_slice();
        let map_facet_fields = round.request.map_facet_fields.as_slice();
        let range_facet_fields = round.request.range_facet_fields.as_slice();
        let score_stages = round.request.score_stages.as_slice();
        let geo_filters = round.request.geo_filters.as_slice();
        let filter = round.request.filter.as_ref();
        let stats_fields = round.request.stats_fields.as_slice();
        let cardinality_fields = round.request.cardinality_fields.as_slice();
        let projections = round.request.projections.as_slice();
        let mut query_tasks = tokio::task::JoinSet::new();
        let stream_enabled = self.bm25_stream && round.phrase.is_none();
        let relay = stream_enabled
            .then(|| watch::channel(min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = stream_enabled.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        let mask = self.shard_mask(filter);
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let mut request = round.request.clone();
            request.expected_stats_epoch = round.claims[shard].epoch;
            request.expected_stats_incarnation = round.claims[shard].incarnation();
            let mut client = self.node_client(node)?;
            let phrase_request =
                round.phrase.map(
                    |(phrase_leg, weights, _)| crate::pb::Bm25PhraseQueryRequest {
                        query: Some(request.clone()),
                        phrase_leg: phrase_leg as u32,
                        phrase_term_weights: weights.to_vec(),
                    },
                );
            let relay = relay.clone();
            let stream_heap = stream_heap.clone();
            let deadline = self.limits.shard_deadline;
            query_tasks.spawn(async move {
                let started = std::time::Instant::now();
                let (response, fingerprint) = if let Some((floor_tx, floor_rx)) = relay {
                    let run = stream_bm25_shard(
                        shard as u32,
                        k as usize,
                        client,
                        request,
                        floor_tx,
                        floor_rx,
                        stream_heap.expect("relay has heap"),
                    );
                    let result = match deadline {
                        Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                            Status::deadline_exceeded(format!(
                                "BM25 shard {shard} exceeded its {}ms deadline",
                                limit.as_millis()
                            ))
                        })?,
                        None => run.await,
                    }?;
                    (result.response, Some(result.scoring_fingerprint))
                } else {
                    let response = match phrase_request {
                        Some(request) => client.bm25_phrase_query(request).await,
                        None => client.bm25_query(request).await,
                    }?;
                    (response.into_inner(), None)
                };
                Ok::<_, Status>(Bm25Reply {
                    shard: shard as u32,
                    response,
                    fingerprint,
                    millis: started.elapsed().as_secs_f64() * 1000.0,
                })
            });
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut stage_known = vec![false; score_stages.len()];
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        // Projection column-read leaves, in the wire's flag order
        // (docs/cel-values.md).
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let mut projection_known = vec![false; projection_leaves.len()];
        let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; projections.len()];
        let mut shard_stats: Vec<Vec<crate::pb::ColumnStats>> = Vec::new();
        let mut shard_distinct: Vec<Vec<crate::pb::FacetDistinct>> = Vec::new();
        let mut scoring_fingerprint: Option<String> = None;
        let mut per_shard = Vec::new();
        let mut responses = Vec::new();
        while let Some(joined) = query_tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.shard);
        for response in responses {
            let Bm25Reply {
                shard,
                response,
                fingerprint,
                millis,
            } = response;
            per_shard.push((shard, millis));
            let Bm25QueryResponse {
                hits,
                facets,
                range_facets: ranges,
                stage_columns_known: known,
                geo_columns_known: geo,
                filter_columns_known: fknown,
                stats: sstats,
                distinct: sdistinct,
                projection_leaves_known: pknown,
                projection_types: ptypes,
                segments_total,
                segments_skipped,
                ..
            } = response;
            let sprune = (segments_total, segments_skipped);
            if let Some(fingerprint) = fingerprint {
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &fingerprint => {
                        return Err(Status::failed_precondition(format!(
                            "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"
                        )));
                    }
                    None => scoring_fingerprint = Some(fingerprint),
                    _ => {}
                }
            }
            if round.flat {
                if pknown.len() != projection_leaves.len() {
                    return Err(Status::internal(format!(
                        "shard answered {} projection-leaf flags for {} leaves",
                        pknown.len(),
                        projection_leaves.len()
                    )));
                }
                for (acc, k) in projection_known.iter_mut().zip(&pknown) {
                    *acc |= *k;
                }
                crate::values::merge_projection_types(projections, &mut projection_types, &ptypes)?;
                for hit in &hits {
                    crate::values::validate_projection_row(&hit.projected, &ptypes)?;
                }
            }
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            shard_stats.push(sstats);
            shard_distinct.push(sdistinct);
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sprune.0,
                segments_skipped: sprune.1,
            });
            if geo.len() != geo_filters.len() {
                return Err(Status::internal(format!(
                    "shard answered {} geo-column flags for {} filters",
                    geo.len(),
                    geo_filters.len()
                )));
            }
            for (acc, k) in geo_known.iter_mut().zip(&geo) {
                *acc |= *k;
            }
            if fknown.len() != filter_leaves {
                return Err(Status::internal(format!(
                    "shard answered {} filter-leaf flags for {} leaves",
                    fknown.len(),
                    filter_leaves
                )));
            }
            for (acc, k) in filter_known.iter_mut().zip(&fknown) {
                *acc |= *k;
            }
            if round.flat {
                if known.len() != score_stages.len() {
                    return Err(Status::internal(format!(
                        "shard answered {} stage-column flags for {} stages",
                        known.len(),
                        score_stages.len()
                    )));
                }
                for (acc, k) in stage_known.iter_mut().zip(known) {
                    *acc |= k;
                }
            }
        }
        // A stage column NO shard knows is a typo wearing an identity
        // chain — the whole request would be a silent no-op. Refuse,
        // naming the column and the knob; a partially-known column is
        // the heterogeneous fleet and is exact (absent = identity).
        let unknown: Vec<String> = score_stages
            .iter()
            .zip(&stage_known)
            .filter(|(_, known)| !**known)
            .map(|(s, _)| match s.map_key() {
                None => format!("{:?}", s.column),
                Some(key) => format!("{:?}[{:?}]", s.column, key),
            })
            .collect();
        if !unknown.is_empty() {
            // A geo decay stage reads a geo column, so pointing the
            // caller at --numeric-fields alone would send them looking
            // in the wrong table for a name they spelled wrong.
            let any_geo = score_stages.iter().zip(&stage_known).any(|(s, known)| {
                !known
                    && matches!(
                        crate::pb::ScoreOp::try_from(s.operation_code()),
                        Ok(crate::pb::ScoreOp::MultGeoDecayHaversine
                            | crate::pb::ScoreOp::MultGeoDecayManhattan)
                    )
            });
            let knobs = if any_geo {
                "--numeric-fields / --integer-fields / --map-numeric-fields / --geo-fields"
            } else {
                "--numeric-fields / --map-numeric-fields"
            };
            return Err(Status::invalid_argument(format!(
                "no shard has numeric column {}: the chain would be a silent no-op. \
                 Check the spelling, or the nodes' {knobs}.",
                unknown.join(", ")
            )));
        }
        refuse_unknown_geo_columns(geo_filters, &geo_known)?;
        if let Some(mask) = mask.as_ref() {
            mark_known(&mut filter_known, &mask.known);
        }
        refuse_unknown_filter_leaves(filter, &filter_known)?;
        // A projection column NO shard knows is a typo answering
        // all-absent — refuse it by name. A partially-known column is
        // the heterogeneous fleet and is exact (absent documents hold
        // nothing).
        let unknown_projection: Vec<String> = projection_leaves
            .iter()
            .zip(&projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown_projection.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown_projection.join(", ")
            )));
        }
        let facets = merge_facet_counts(facet_fields, map_facet_fields, &shard_facets, true)?;
        let ranges = merge_range_counts(range_facet_fields, &shard_ranges)?;
        let stats = if round.flat {
            merge_column_stats(stats_fields, &shard_stats)?
        } else {
            Vec::new()
        };
        let cardinality = if round.flat {
            merge_cardinality(cardinality_fields, &shard_distinct)?
        } else {
            Vec::new()
        };
        if let (Some(heap), Some(fingerprint)) = (&stream_heap, scoring_fingerprint.as_ref()) {
            let snapshot = heap
                .lock()
                .expect("BM25 stream heap poisoned")
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect();
            self.publish_progress(
                crate::pb::QueryStreamPhase::Lexical,
                snapshot,
                fingerprint.clone(),
            );
        }
        let t_query = round.timing.map(|t| t.timing.started.elapsed());
        if round.timing.is_some_and(|t| t.timing.trace) {
            eprintln!(
                "bm25-fused per-shard ms: {}",
                per_shard
                    .iter()
                    .map(|(s, ms)| format!("{s}:{ms:.0}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        let hits = finish_bm25_hits(all, stream_heap, k)?;
        if let Some(t) = round.timing.filter(|t| t.timing.trace) {
            let t_query = t_query.expect("fused round has timing");
            let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
            eprintln!("bm25-fused: analyze {:.1} ms, term-stats {:.1} ms, query fan-out {:.1} ms, merge {:.1} ms  ({} fields, {} terms)", ms(t.timing.analyzed), ms(t.timing.stats - t.timing.analyzed), ms(t_query - t.timing.stats), ms(t.timing.started.elapsed() - t_query), t.fields, t.terms);
        }
        Ok((hits, facets, ranges, stats, cardinality, prune))
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
            winners.sort_by(cmp_hits);
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
