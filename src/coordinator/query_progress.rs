//! Query progress.

use super::*;

/// Shared revision state and response sink for one query-stream attempt.
pub(super) struct QueryStreamOutput<'a> {
    pub(super) tx: &'a mpsc::Sender<Result<crate::pb::QueryStreamResponse, Status>>,
    pub(super) revision: &'a mut u64,
    pub(super) scoring_fingerprints: &'a mut Vec<String>,
    pub(super) request_fingerprint: &'a str,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct QueryProgress {
    pub(super) phase: crate::pb::QueryStreamPhase,
    pub(super) hits: Vec<(u64, f32)>,
    pub(super) scoring_fingerprint: String,
}

pub(super) struct QueryProgressPublisher {
    pub(super) sender: watch::Sender<Option<QueryProgress>>,
    // Set before selection. This reader has no progress publisher, so the
    // retained read context cannot form a cycle through the watch channel.
    pub(super) reader: std::sync::OnceLock<Arc<CoordinatorServiceImpl>>,
}

pub(super) fn query_stream_content_fingerprint(
    phase: crate::pb::QueryStreamPhase,
    hits: &[crate::pb::QueryStreamHit],
    identity_state: crate::pb::QueryStreamIdentityState,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"protomolt-query-revision-v2\0");
    bytes.extend_from_slice(&(phase as i32).to_le_bytes());
    bytes.extend_from_slice(&(identity_state as i32).to_le_bytes());
    for hit in hits {
        bytes.extend_from_slice(&hit.doc_id.to_le_bytes());
        // The implicit protobuf float field omits both signed zeros. Hash
        // the value a decoder receives, including for in-process clients.
        let score_bits = if hit.score == 0.0 {
            0
        } else {
            hit.score.to_bits()
        };
        bytes.extend_from_slice(&score_bits.to_le_bytes());
        bytes.extend_from_slice(&hit.rank.to_le_bytes());
        match &hit.identity {
            None => bytes.push(0),
            Some(identity) => {
                bytes.push(1);
                bytes.extend_from_slice(&(identity.document_key.len() as u64).to_le_bytes());
                bytes.extend_from_slice(&identity.document_key);
                bytes.extend_from_slice(&identity.version.to_le_bytes());
                match identity.chunk_ordinal {
                    None => bytes.push(0),
                    Some(ordinal) => {
                        bytes.push(1);
                        bytes.extend_from_slice(&ordinal.to_le_bytes());
                    }
                }
            }
        }
    }
    crate::sha256::hex_digest(&bytes)
}

pub(super) fn query_stream_revision(
    revision: u64,
    phase: crate::pb::QueryStreamPhase,
    hits: Vec<(u64, f32, Option<crate::pb::DocumentIdentity>)>,
    scoring_fingerprint: String,
    identity_state: crate::pb::QueryStreamIdentityState,
) -> crate::pb::QueryStreamRevision {
    let hits = hits
        .into_iter()
        .enumerate()
        .map(
            |(rank, (doc_id, score, identity))| crate::pb::QueryStreamHit {
                doc_id,
                score,
                rank: rank as u32 + 1,
                identity: if identity_state == crate::pb::QueryStreamIdentityState::Resolved {
                    identity
                } else {
                    None
                },
            },
        )
        .collect::<Vec<_>>();
    let content_fingerprint = query_stream_content_fingerprint(phase, &hits, identity_state);
    crate::pb::QueryStreamRevision {
        revision,
        phase: phase as i32,
        hits,
        content_fingerprint,
        scoring_fingerprint,
        identity_state: identity_state as i32,
        content_fingerprint_version: 2,
    }
}

pub(super) fn combined_scoring_fingerprint(
    fingerprints: &[String],
    request_fallback: &str,
) -> String {
    if fingerprints.is_empty() {
        return request_fallback.to_string();
    }
    let mut canonical = fingerprints.to_vec();
    canonical.sort();
    canonical.dedup();
    crate::sha256::hex_digest(canonical.join("\0").as_bytes())
}

impl CoordinatorServiceImpl {
    pub(super) fn with_query_progress(
        mut self,
        progress: watch::Sender<Option<QueryProgress>>,
    ) -> Self {
        self.query_progress = Some(Arc::new(QueryProgressPublisher {
            sender: progress,
            reader: std::sync::OnceLock::new(),
        }));
        self
    }

    pub(super) fn publish_progress(
        &self,
        phase: crate::pb::QueryStreamPhase,
        mut hits: Vec<(u64, f32)>,
        scoring_fingerprint: impl Into<String>,
    ) {
        let Some(progress) = self.query_progress.as_ref() else {
            return;
        };
        hits.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let next = QueryProgress {
            phase,
            hits,
            scoring_fingerprint: scoring_fingerprint.into(),
        };
        progress.sender.send_if_modified(|current| {
            if current.as_ref() == Some(&next) {
                false
            } else {
                *current = Some(next);
                true
            }
        });
    }

    /// Read fresh versions without using the statistics cache. A version-only
    /// TermStats request is supported by nodes and relays, including shards
    /// without lexical rows. Its version covers the shard's physical rows.
    pub(super) async fn read_query_versions(
        &self,
        allow_replicas: bool,
    ) -> Result<Vec<(String, StatsClaim)>, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut known = vec![false; scope.column_count()];
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let primary = (node.clone(), self.node_client(node)?);
            let replica = if allow_replicas {
                self.replica_addrs
                    .get(shard)
                    .and_then(Option::as_ref)
                    .map(|addr| self.node_client(addr).map(|link| (addr.clone(), link)))
                    .transpose()?
            } else {
                None
            };
            let visibility = self.document_visibility.clone();
            let limits = self.limits();
            tasks.spawn(async move {
                async fn read(
                    (address, mut client): (String, crate::link::NodeLink),
                    visibility: Option<crate::pb::DocumentVisibility>,
                ) -> Result<(String, crate::pb::TermStatsResponse), Status> {
                    let response = client.term_stats(TermStatsRequest {
                        version_only: true,
                        terms: Vec::new(), fields: Vec::new(), visibility,
                    }).await?.into_inner();
                    Ok((address, response))
                }
                let attempt = async move {
                    let mut primary = Box::pin(read(primary, visibility.clone()));
                    let Some(replica) = replica else { return primary.await; };
                    if let Some(delay) = limits.hedge_delay {
                        match tokio::time::timeout(delay, &mut primary).await {
                            Ok(Ok(response)) => return Ok(response),
                            Ok(Err(_)) => return read(replica, visibility).await,
                            Err(_) => {}
                        }
                        let mut replica = Box::pin(read(replica, visibility));
                        tokio::select! {
                            result = &mut primary => match result { Ok(response) => Ok(response), Err(_) => replica.await },
                            result = &mut replica => match result { Ok(response) => Ok(response), Err(_) => primary.await },
                        }
                    } else {
                        match primary.await {
                            Ok(response) => Ok(response),
                            Err(_) => read(replica, visibility).await,
                        }
                    }
                };
                let result = match limits.shard_deadline {
                    Some(deadline) => tokio::time::timeout(deadline, attempt).await
                        .unwrap_or_else(|_| Err(Status::deadline_exceeded("query version probe exceeded the shard deadline"))),
                    None => attempt.await,
                };
                (shard, result)
            });
        }
        let mut versions = vec![None; self.node_addrs.len()];
        while let Some(task) = tasks.join_next().await {
            let (shard, response) = task
                .map_err(|error| Status::internal(format!("query version task failed: {error}")))?;
            let (address, response) = response?;
            scope.validate_response(&response)?;
            crate::visibility::validate_stats_mode(true, &response)?;
            for (known, present) in known.iter_mut().zip(response.visibility_columns_known) {
                *known |= present;
            }
            versions[shard] = Some((
                address,
                StatsClaim::required(response.stats_epoch, &response.stats_incarnation)?,
            ));
        }
        self.check_visibility_columns(&known)?;
        Ok(versions
            .into_iter()
            .map(|version| version.expect("every version task completed"))
            .collect())
    }

    pub(crate) fn admitted_read_claims(&self) -> Result<Vec<StatsClaim>, Status> {
        self.query_read_versions
            .as_ref()
            .map(|claims| claims.as_ref().clone())
            .ok_or_else(|| Status::failed_precondition("query has no admitted physical read set"))
    }

    pub(super) async fn pin_read_versions(
        &self,
    ) -> Result<(Self, Vec<(String, StatsClaim)>), Status> {
        let reads = self.read_query_versions(true).await?;
        let mut pinned = self.clone();
        pinned.node_addrs = reads.iter().map(|(address, _)| address.clone()).collect();
        pinned.replica_addrs.clear();
        pinned.query_read_versions =
            Some(Arc::new(reads.iter().map(|(_, claim)| *claim).collect()));
        Ok((pinned, reads))
    }

    pub(super) async fn validate_read_versions(
        &self,
        reads: &[(String, StatsClaim)],
    ) -> Result<(), Status> {
        if self.read_query_versions(false).await? != reads {
            return Err(Status::failed_precondition(
                "query data changed during execution; restart from the first page",
            ));
        }
        Ok(())
    }

    /// Capture before selection and validate after every phase. Every shard's
    /// unchanged interval includes the interval between the two fan-outs;
    /// mutations or lifetime replacement invalidate the response, never retry
    /// just its value fetch against a newer version.
    pub(super) async fn execute_query(
        &self,
        mut request: crate::pb::QueryRequest,
        access: Option<&crate::pb::AccessDecision>,
        finish_deadline_us: u64,
    ) -> Result<crate::pb::QueryResponse, Status> {
        let disclosure = self
            .field_permissions
            .as_ref()
            .map(|fields| fields.query(&request))
            .transpose()?;
        let started = std::time::Instant::now();
        let mut cursor = self.bind_query_cursor(&mut request, access)?;
        let (scoped, reads) = self.pin_read_versions().await?;
        cursor.bind_read_versions(
            scoped
                .query_read_versions
                .as_ref()
                .expect("pinned read versions"),
        )?;
        if let Some(publisher) = &self.query_progress {
            let mut reader = scoped.clone();
            reader.query_progress = None;
            publisher
                .reader
                .set(Arc::new(reader))
                .map_err(|_| Status::internal("query stream read context was initialized twice"))?;
        }
        let result = async {
            let mut response = crate::query::execute(&scoped, request, finish_deadline_us).await?;
            if self
                .field_permissions
                .as_ref()
                .is_none_or(|fields| fields.can_disclose_identity())
            {
                scoped.fill_query_identities(&mut response).await?;
            }
            Ok::<_, Status>(response)
        }
        .await;
        scoped.validate_read_versions(&reads).await?;
        let mut response = result?;
        if let Some(disclosure) = disclosure {
            disclosure.apply(&mut response);
        }
        if self.document_visibility.is_some() {
            crate::query_disclosure::redact_execution(&mut response);
        }
        cursor.finish(&mut response)?;
        response.served_topology_generation = scoped.topology_generation;
        if let Some(profile) = response.profile.as_mut() {
            profile.total_ms = started.elapsed().as_secs_f32() * 1000.0;
        }
        Ok(response)
    }

    pub(super) async fn fill_query_identities(
        &self,
        response: &mut crate::pb::QueryResponse,
    ) -> Result<(), Status> {
        let ids: Vec<_> = response
            .hits
            .iter()
            .chain(response.groups.iter().flat_map(|group| &group.hits))
            .map(|hit| hit.doc_id)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let claims = self.admitted_read_claims()?;
        let identities = self.resolve_candidate_identities_at(&ids, &claims).await?;
        if identities.len() != ids.len() {
            return Err(Status::failed_precondition(
                "query identity fetch omitted a selected row",
            ));
        }
        for hit in response
            .hits
            .iter_mut()
            .chain(response.groups.iter_mut().flat_map(|group| &mut group.hits))
        {
            let identity = identities
                .get(&hit.doc_id)
                .expect("complete candidate identity set");
            if hit.identity.is_some() && &hit.identity != identity {
                return Err(Status::failed_precondition(
                    "query identity differs from the selection identity",
                ));
            }
            hit.identity = identity.clone();
        }
        Ok(())
    }

    pub(super) async fn resolve_query_progress(
        &self,
        progress: QueryProgress,
        revision: u64,
    ) -> Result<crate::pb::QueryStreamRevision, Status> {
        let reader = self
            .query_progress
            .as_ref()
            .and_then(|publisher| publisher.reader.get())
            .ok_or_else(|| {
                Status::failed_precondition("query progress has no admitted read context")
            })?;
        let claims = reader.admitted_read_claims()?;
        let disclose = reader
            .field_permissions
            .as_ref()
            .is_none_or(|fields| fields.can_disclose_identity());
        let ids: Vec<_> = progress.hits.iter().map(|hit| hit.0).collect();
        let identities = if disclose {
            reader.resolve_candidate_identities_at(&ids, &claims).await
        } else {
            Ok(HashMap::new())
        };
        let reads: Vec<_> = reader.node_addrs.iter().cloned().zip(claims).collect();
        reader.validate_read_versions(&reads).await?;
        let identities = identities?;
        if disclose
            && (identities.len() != ids.len() || ids.iter().any(|id| !identities.contains_key(id)))
        {
            return Err(Status::failed_precondition(
                "stream identity fetch omitted a candidate",
            ));
        }
        let hits = progress
            .hits
            .into_iter()
            .map(|(id, score)| (id, score, identities.get(&id).cloned().flatten()))
            .collect();
        Ok(query_stream_revision(
            revision,
            progress.phase,
            hits,
            progress.scoring_fingerprint,
            if disclose {
                crate::pb::QueryStreamIdentityState::Resolved
            } else {
                crate::pb::QueryStreamIdentityState::Withheld
            },
        ))
    }

    pub(super) async fn query_stream_attempt(
        &self,
        query: crate::pb::QueryRequest,
        access: Option<crate::pb::AccessDecision>,
        finish_deadline_us: u64,
        mut progress_rx: watch::Receiver<Option<QueryProgress>>,
        output: QueryStreamOutput<'_>,
    ) -> Result<crate::pb::QueryResponse, Status> {
        let QueryStreamOutput {
            tx,
            revision,
            scoring_fingerprints,
            request_fingerprint,
        } = output;
        let runner = self.clone();
        let mut execution = tokio::task::JoinSet::new();
        execution.spawn(async move {
            runner
                .execute_query(query, access.as_ref(), finish_deadline_us)
                .await
        });
        let mut last_content_fingerprint: Option<String> = None;
        let mut progress_open = true;
        loop {
            tokio::select! {
                changed = progress_rx.changed(), if progress_open => {
                    if changed.is_err() { progress_open = false; continue; }
                    let Some(progress) = progress_rx.borrow_and_update().clone() else { continue; };
                    if !progress.scoring_fingerprint.is_empty() && !scoring_fingerprints.contains(&progress.scoring_fingerprint) {
                        scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                    }
                    let snapshot = self.resolve_query_progress(progress, *revision + 1).await?;
                    if last_content_fingerprint.as_ref() == Some(&snapshot.content_fingerprint) { continue; }
                    let fingerprint = snapshot.content_fingerprint.clone();
                    let event = crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                    };
                    match tx.try_send(Ok(event)) {
                        Ok(()) => { *revision += 1; last_content_fingerprint = Some(fingerprint); }
                        Err(mpsc::error::TrySendError::Full(_)) => {},
                        Err(mpsc::error::TrySendError::Closed(_)) => return Err(Status::cancelled("query stream closed")),
                    }
                }
                joined = execution.join_next() => {
                    let mut response = joined.ok_or_else(|| Status::internal("query execution task disappeared"))?
                        .map_err(|_| Status::internal("query execution task failed"))??;
                    // Preserve the last collector order even when execution and
                    // progress become ready together. Identity reads still use
                    // the original admitted context, never a fresh generation.
                    let pending = progress_rx.borrow().clone();
                    if let Some(progress) = pending {
                        if !progress.scoring_fingerprint.is_empty() && !scoring_fingerprints.contains(&progress.scoring_fingerprint) {
                            scoring_fingerprints.push(progress.scoring_fingerprint.clone());
                        }
                        let snapshot = self.resolve_query_progress(progress, *revision + 1).await?;
                        if last_content_fingerprint.as_ref() != Some(&snapshot.content_fingerprint) {
                            tx.send(Ok(crate::pb::QueryStreamResponse {
                                payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                            })).await.map_err(|_| Status::cancelled("query stream closed"))?;
                            *revision += 1;
                        }
                    }
                    response.served_topology_generation = self.topology_generation;
                    scoring_fingerprints.sort(); scoring_fingerprints.dedup();
                    let final_scoring = combined_scoring_fingerprint(scoring_fingerprints, request_fingerprint);
                    let final_hits = response.hits.iter().map(|hit| (hit.doc_id, hit.score, hit.identity.clone())).collect();
                    let state = if self.field_permissions.as_ref().is_some_and(|fields| !fields.can_disclose_identity()) {
                        crate::pb::QueryStreamIdentityState::Withheld
                    } else { crate::pb::QueryStreamIdentityState::Resolved };
                    let snapshot = query_stream_revision(*revision + 1, crate::pb::QueryStreamPhase::Final,
                        final_hits, final_scoring, state);
                    tx.send(Ok(crate::pb::QueryStreamResponse {
                        payload: Some(crate::pb::query_stream_response::Payload::Revision(snapshot)),
                    })).await.map_err(|_| Status::cancelled("query stream closed"))?;
                    *revision += 1;
                    return Ok(response);
                }
            }
        }
    }

    pub(super) fn bind_query_cursor(
        &self,
        request: &mut crate::pb::QueryRequest,
        access: Option<&crate::pb::AccessDecision>,
    ) -> Result<crate::query_cursor::CursorBinding, Status> {
        self.admit(&request.collection)?;
        let routes = self
            .current_topology_routes()
            .into_iter()
            .map(|route| crate::pb::QueryCursorRoute {
                address: route.addr,
                replica: route.replica,
                hash_start: route.hash_range.map(|range| range.0),
                hash_end: route.hash_range.map(|range| range.1),
                placement: route.placement,
            })
            .collect();
        crate::query_cursor::CursorBinding::prepare(
            self.cursor_signer.clone(),
            request,
            crate::pb::QueryCursorContext {
                query_sha256: Vec::new(),
                collection: self.collection.clone(),
                access: access.cloned(),
                topology_generation: self.topology_generation,
                routes,
            },
        )
    }
}
