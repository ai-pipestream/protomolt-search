//! Mapped document ingestion under topology and write barriers.

use super::*;

impl CoordinatorServiceImpl {
    /// The first message of a routed mapped ingest stream, which must be
    /// the bind: it names the collection the whole stream writes into
    /// (`docs/collections.md`).
    pub async fn routed_bind(
        inbound: &mut (impl tokio_stream::Stream<Item = Result<RoutedIngestMappedRequest, Status>>
                  + Unpin),
    ) -> Result<crate::pb::RoutedMappedBind, Status> {
        match tokio_stream::StreamExt::next(inbound).await.transpose()? {
            Some(RoutedIngestMappedRequest {
                payload: Some(crate::pb::routed_ingest_mapped_request::Payload::Bind(bind)),
            }) => Ok(bind),
            _ => Err(Status::invalid_argument(
                "first RoutedIngestMappedRequest must be a RoutedMappedBind",
            )),
        }
    }

    /// Routed mapped ingest after its bind was read (and, on a collection
    /// set, resolved): the write gate, the topology snapshot, and the
    /// per-shard fan-out.
    pub async fn routed_ingest_mapped_bound<S>(
        &self,
        bind: crate::pb::RoutedMappedBind,
        mut inbound: S,
    ) -> Result<RoutedIngestMappedResponse, Status>
    where
        S: tokio_stream::Stream<Item = Result<RoutedIngestMappedRequest, Status>> + Unpin + Send,
    {
        // Gate before snapshotting: a write that arrived during a cutover
        // must resume into the new map, never retain the old snapshot while
        // waiting behind the final-tail barrier.
        let _write_guard = if self.live_topology.is_some() {
            Some(self.write_gate.clone().read_owned().await)
        } else {
            None
        };
        if let Some(snapshot) = self.request_snapshot() {
            return Box::pin(snapshot.routed_ingest_mapped_bound(bind, inbound)).await;
        }
        if bind.required_topology_generation == 0 {
            return Err(Status::invalid_argument(
                "routed writes require required_topology_generation; zero is not accepted",
            ));
        }
        self.require_topology_generation(bind.required_topology_generation)?;
        // Ingest lands rows on shards that must score alike: a fleet that
        // already scores in two spaces is refused before a row moves.
        self.fleet_vector_identity(false).await?;
        let mapped_bind = bind
            .bind
            .ok_or_else(|| Status::invalid_argument("routed mapped bind is missing bind"))?;
        // Under a placement tree the coordinator evaluates the tree over
        // each document's own columns and routes inside the leaf it
        // picks (docs/placement.md); the leaf's shards fill the
        // placement column from their pinned code.
        let placer = match self.placement.as_ref() {
            Some(placement) => Some(PlacementRouter::new(
                Arc::clone(placement),
                &mapped_bind,
                self.derived.clone(),
            )?),
            None => None,
        };
        let mut batches: Vec<Vec<crate::pb::IngestMappedRequest>> =
            vec![Vec::new(); self.node_addrs.len()];
        let mut position = 0u64;
        while let Some(message) = tokio_stream::StreamExt::next(&mut inbound)
            .await
            .transpose()?
        {
            let document = match message.payload {
                Some(crate::pb::routed_ingest_mapped_request::Payload::Document(document)) => {
                    document
                }
                Some(crate::pb::routed_ingest_mapped_request::Payload::Bind(_)) => {
                    return Err(Status::invalid_argument(
                        "routed mapped bind repeats mid-stream",
                    ))
                }
                None => {
                    return Err(Status::invalid_argument(
                        "empty RoutedIngestMappedRequest payload",
                    ))
                }
            };
            let leaf = match placer.as_ref() {
                Some(placer) => {
                    Some(placer.leaf_of(&document.document, position, &document.stable_key)?)
                }
                None => None,
            };
            position += 1;
            let (_, shard) = self
                .route_stable_key_in(&document.stable_key, leaf)
                .map_err(Status::invalid_argument)?;
            if batches[shard].is_empty() {
                batches[shard].push(crate::pb::IngestMappedRequest {
                    payload: Some(crate::pb::ingest_mapped_request::Payload::Bind(
                        mapped_bind.clone(),
                    )),
                });
            }
            batches[shard].push(crate::pb::IngestMappedRequest {
                payload: Some(crate::pb::ingest_mapped_request::Payload::RoutedDocument(
                    document,
                )),
            });
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (shard, batch) in batches.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let addr = self.node_addrs[shard].clone();
            let mut client = self.node_client(&addr)?;
            tasks.spawn(async move {
                let response = client
                    .ingest_mapped(tokio_stream::iter(batch))
                    .await?
                    .into_inner();
                Ok::<_, Status>((shard, addr, response))
            });
        }
        let mut shards = Vec::new();
        let mut added = 0u64;
        let mut parents = 0u64;
        while let Some(result) = tasks.join_next().await {
            let (shard, addr, response) = result.map_err(|error| {
                Status::internal(format!("routed ingest task failed: {error}"))
            })??;
            added = added.saturating_add(response.added);
            parents = parents.saturating_add(response.parents);
            shards.push(RoutedShardIngest {
                shard: shard as u32,
                addr,
                added: response.added,
                parents: response.parents,
                first_id: response.first_id,
            });
        }
        shards.sort_by_key(|result| result.shard);
        Ok(RoutedIngestMappedResponse {
            added,
            parents,
            served_topology_generation: self.topology_generation,
            shards,
        })
    }
}
