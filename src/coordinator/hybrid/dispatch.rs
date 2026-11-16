//! Per-shard dispatch of the hybrid legs and local fusion requests.

use super::super::hybrid_context::{local_fusion_request, HybridRound};
use super::*;

pub(super) enum LocalHybridReply {
    Clustered {
        response: crate::pb::ShardLegsResponse,
        vector: Vec<(u64, f64)>,
    },
    Plain(crate::pb::HybridShardResponse),
}

type HybridTask<T> = tokio::task::JoinHandle<Result<(u32, f32, T), Status>>;

impl CoordinatorServiceImpl {
    pub(super) fn spawn_global_hybrid(
        &self,
        round: HybridRound<'_>,
        mask: Option<&crate::placement::ShardMask>,
        admission: &Option<Arc<crate::vector_read::VectorReadBarrier>>,
        clustered: bool,
    ) -> Result<Vec<HybridTask<crate::pb::ShardLegsResponse>>, Status> {
        let query = round.query();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.is_some_and(|mask| mask.skipped[shard]) {
                continue;
            }
            let request = round.shard_request(
                self.bm25_params,
                shard,
                admission
                    .as_ref()
                    .map(|barrier| barrier.context(shard))
                    .transpose()?,
                Self::shard_filter_tree(query.input().filters(), mask, shard),
                clustered,
                false,
            );
            let mut client = self.node_client(node)?;
            tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                client.shard_legs(request).await.map(|response| {
                    (
                        shard as u32,
                        started.elapsed().as_secs_f32() * 1e3,
                        response.into_inner(),
                    )
                })
            }));
        }
        Ok(tasks)
    }

    pub(super) fn spawn_local_hybrid(
        &self,
        round: HybridRound<'_>,
        mask: Option<&crate::placement::ShardMask>,
        admission: &Option<Arc<crate::vector_read::VectorReadBarrier>>,
        clustered: bool,
        vector_legs: &[Vec<(u64, f64)>],
    ) -> Result<Vec<HybridTask<LocalHybridReply>>, Status> {
        let query = round.query();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.is_some_and(|mask| mask.skipped[shard]) {
                continue;
            }
            let request = round.shard_request(
                self.bm25_params,
                shard,
                admission
                    .as_ref()
                    .map(|barrier| barrier.context(shard))
                    .transpose()?,
                Self::shard_filter_tree(query.input().filters(), mask, shard),
                clustered,
                true,
            );
            let mut client = self.node_client(node)?;
            let local_vectors = clustered.then(|| vector_legs[shard].clone());
            let legs = query.legs();
            tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                if let Some(vector) = local_vectors {
                    let response = client.shard_legs(request).await?.into_inner();
                    let elapsed = started.elapsed().as_secs_f32() * 1e3;
                    Ok((
                        shard as u32,
                        elapsed,
                        LocalHybridReply::Clustered { response, vector },
                    ))
                } else {
                    let response = client
                        .hybrid_shard(local_fusion_request(request, legs))
                        .await?
                        .into_inner();
                    Ok::<_, Status>((
                        shard as u32,
                        started.elapsed().as_secs_f32() * 1e3,
                        LocalHybridReply::Plain(response),
                    ))
                }
            }));
        }
        Ok(tasks)
    }
}
