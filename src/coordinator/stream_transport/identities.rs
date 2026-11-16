//! Post-scan identity exchange over an open stream fan-out.

use super::*;

/// Identities and rerank candidates keyed by `(shard, vector_id)`.
type IdentityMaps = (
    HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>,
    HashMap<(u32, u64), Option<crate::pb::RerankCandidatePayload>>,
);

/// Rows requested from each shard and the answers received so far.
struct IdentityExchange<'a> {
    expected: Vec<Vec<u64>>,
    received: Vec<bool>,
    identities: HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>,
    candidates: HashMap<(u32, u64), Option<crate::pb::RerankCandidatePayload>>,
    payload_bytes: usize,
    limits: &'a crate::pb::StreamIdentityLimits,
}

impl<'a> IdentityExchange<'a> {
    fn new(shards: usize, hits: &[MergedHit], limits: &'a crate::pb::StreamIdentityLimits) -> Self {
        let mut expected = vec![Vec::new(); shards];
        for hit in hits {
            expected[hit.shard as usize].push(hit.vector_id);
        }
        Self {
            received: vec![false; expected.len()],
            expected,
            identities: HashMap::with_capacity(hits.len()),
            candidates: HashMap::with_capacity(hits.len()),
            payload_bytes: 0,
            limits,
        }
    }

    /// Accept one shard's identity rows: exactly the requested ids in the
    /// requested order, once, within the combined response budget.
    fn absorb(&mut self, shard: usize, found: crate::pb::StreamIdentities) -> Result<(), Status> {
        if self.received[shard] || found.rows.len() != self.expected[shard].len() {
            return Err(Status::internal(
                "shard identity response count or sequence differs",
            ));
        }
        for (row, expected_id) in found.rows.into_iter().zip(&self.expected[shard]) {
            if row.vector_id != *expected_id {
                return Err(Status::internal("shard returned another ID's identity"));
            }
            let len = prost::Message::encoded_len(&row);
            self.payload_bytes = self
                .payload_bytes
                .checked_add(1 + prost::encoding::encoded_len_varint(len as u64) + len)
                .ok_or_else(|| Status::resource_exhausted("identity response length overflow"))?;
            let response_bytes = 1
                + prost::encoding::encoded_len_varint(self.payload_bytes as u64)
                + self.payload_bytes;
            if response_bytes > self.limits.max_response_bytes as usize {
                return Err(Status::resource_exhausted(
                    "combined identity response exceeds max_response_bytes",
                ));
            }
            self.identities
                .insert((shard as u32, row.vector_id), row.identity);
            self.candidates
                .insert((shard as u32, row.vector_id), row.rerank_candidate);
        }
        self.received[shard] = true;
        Ok(())
    }
}

impl StreamFanout {
    #[cfg(test)]
    pub(crate) async fn resolve_identities(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<HashMap<(u32, u64), Option<crate::pb::DocumentIdentity>>, Status> {
        self.resolve_identities_and_candidates(hits, scans, terminal, limits)
            .await
            .map(|(identities, _)| identities)
    }

    pub(crate) async fn resolve_identities_and_candidates(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<IdentityMaps, Status> {
        let exchange = self.exchange_identities(hits, scans, terminal, limits);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(u64::from(limits.timeout_ms)),
            exchange,
        )
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("identity fan-out timed out")));
        match result {
            Ok(pair) => Ok(pair),
            Err(status) => self.cancel_with(status).await,
        }
    }

    /// Ask every open shard for its hits' identities; returns how many
    /// shards were asked.
    async fn request_identities(&self, expected: &[Vec<u64>]) -> Result<usize, Status> {
        let mut remaining = 0usize;
        for (shard, tx) in self.floor_txs.iter().enumerate() {
            if let Some(tx) = tx {
                remaining += 1;
                tx.send(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::ResolveIdentities(
                        crate::pb::ResolveStreamIdentities {
                            vector_ids: expected[shard].clone(),
                        },
                    )),
                })
                .await
                .map_err(|_| Status::unavailable("identity request stream closed"))?;
            }
        }
        Ok(remaining)
    }

    async fn exchange_identities(
        &mut self,
        hits: &[MergedHit],
        scans: &[Option<StreamSearchSummary>],
        terminal: &mut [Option<StreamSearchSummary>],
        limits: &crate::pb::StreamIdentityLimits,
    ) -> Result<IdentityMaps, Status> {
        let mut exchange = IdentityExchange::new(self.floor_txs.len(), hits, limits);
        let mut remaining = self.request_identities(&exchange.expected).await?;
        while remaining > 0 {
            let Some((shard, message)) = self.next_message(terminal).await? else {
                continue;
            };
            if terminal[shard].is_some() {
                return Err(Status::internal("identity message after terminal summary"));
            }
            if matches!(
                message.payload.as_ref(),
                Some(stream_search_response::Payload::Identities(_))
            ) && prost::Message::encoded_len(&message) > limits.max_response_bytes as usize
            {
                return Err(Status::resource_exhausted(
                    "shard exceeded the identity response budget",
                ));
            }
            match message.payload {
                Some(stream_search_response::Payload::Identities(found)) => {
                    exchange.absorb(shard, found)?;
                }
                Some(stream_search_response::Payload::Summary(summary)) => {
                    if !exchange.received[shard] || Some(&summary) != scans[shard].as_ref() {
                        return Err(Status::failed_precondition(
                            "identity exchange did not certify the captured scan",
                        ));
                    }
                    terminal[shard] = Some(summary);
                    self.mark_completed(shard);
                    remaining -= 1;
                }
                _ => {
                    return Err(Status::internal(
                        "unexpected message during identity selection",
                    ))
                }
            }
        }
        Ok((exchange.identities, exchange.candidates))
    }
}
