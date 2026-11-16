//! Fresh physical read versions and their admission around one query.

use super::*;

type VersionRead = (String, crate::pb::TermStatsResponse);

async fn read_version(
    (address, mut client): (String, crate::link::NodeLink),
    visibility: Option<crate::pb::DocumentVisibility>,
) -> Result<VersionRead, Status> {
    let response = client
        .term_stats(TermStatsRequest {
            version_only: true,
            terms: Vec::new(),
            fields: Vec::new(),
            visibility,
        })
        .await?
        .into_inner();
    Ok((address, response))
}

/// Read from the primary, falling back to (or hedging with) the replica.
async fn read_version_hedged(
    primary: (String, crate::link::NodeLink),
    replica: Option<(String, crate::link::NodeLink)>,
    visibility: Option<crate::pb::DocumentVisibility>,
    limits: FanoutLimits,
) -> Result<VersionRead, Status> {
    let mut primary = Box::pin(read_version(primary, visibility.clone()));
    let Some(replica) = replica else {
        return primary.await;
    };
    if let Some(delay) = limits.hedge_delay {
        match tokio::time::timeout(delay, &mut primary).await {
            Ok(Ok(response)) => return Ok(response),
            Ok(Err(_)) => return read_version(replica, visibility).await,
            Err(_) => {}
        }
        let mut replica = Box::pin(read_version(replica, visibility));
        tokio::select! {
            result = &mut primary => match result { Ok(response) => Ok(response), Err(_) => replica.await },
            result = &mut replica => match result { Ok(response) => Ok(response), Err(_) => primary.await },
        }
    } else {
        match primary.await {
            Ok(response) => Ok(response),
            Err(_) => read_version(replica, visibility).await,
        }
    }
}

impl CoordinatorServiceImpl {
    /// Read fresh versions without using the statistics cache. A version-only
    /// TermStats request is supported by nodes and relays, including shards
    /// without lexical rows. Its version covers the shard's physical rows.
    pub(in crate::coordinator) async fn read_query_versions(
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
                let attempt = read_version_hedged(primary, replica, visibility, limits);
                let result = match limits.shard_deadline {
                    Some(deadline) => tokio::time::timeout(deadline, attempt)
                        .await
                        .unwrap_or_else(|_| {
                            Err(Status::deadline_exceeded(
                                "query version probe exceeded the shard deadline",
                            ))
                        }),
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

    pub(in crate::coordinator) async fn pin_read_versions(
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

    pub(in crate::coordinator) async fn validate_read_versions(
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
}
