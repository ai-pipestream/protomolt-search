//! Admission barrier for a trusted planner's physical vector read set.
use crate::{pb::*, stats_identity::StatsClaim, visibility::VisibilityScope};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tonic::Status;

pub(crate) fn check_binding(
    field: &str,
    actual: Option<&MappedVectorBinding>,
    held: &mut Option<Option<MappedVectorBinding>>,
) -> Result<(), Status> {
    if let Some(binding) = actual {
        crate::mapped_vector::validate(binding, &binding.plan_fingerprint).map_err(|error| {
            Status::failed_precondition(format!("invalid node vector binding: {}", error.message()))
        })?;
    }
    if !field.is_empty() && actual.is_none_or(|binding| binding.field != field) {
        return Err(Status::failed_precondition("node did not acknowledge the requested vector field; use matching node and coordinator builds"));
    }
    if let Some(expected) = held {
        if expected.as_ref() != actual {
            return Err(Status::failed_precondition(
                "vector reads crossed incompatible field bindings",
            ));
        }
    } else {
        *held = Some(actual.cloned());
    }
    Ok(())
}

pub(crate) trait ScanReadFrame {
    fn receipt(self) -> Result<VectorReadReceipt, Status>;
    fn is_receipt(&self) -> bool;
}
impl ScanReadFrame for SearchShardResponse {
    fn receipt(self) -> Result<VectorReadReceipt, Status> {
        match self.payload {
            Some(search_shard_response::Payload::ReadReady(receipt)) => Ok(receipt),
            _ => Err(Status::failed_precondition(
                "vector shard emitted data before ReadReady",
            )),
        }
    }
    fn is_receipt(&self) -> bool {
        matches!(
            self.payload,
            Some(search_shard_response::Payload::ReadReady(_))
        )
    }
}
impl ScanReadFrame for StreamSearchResponse {
    fn receipt(self) -> Result<VectorReadReceipt, Status> {
        match self.payload {
            Some(stream_search_response::Payload::ReadReady(receipt)) => Ok(receipt),
            _ => Err(Status::failed_precondition(
                "vector stream emitted data before ReadReady",
            )),
        }
    }
    fn is_receipt(&self) -> bool {
        matches!(
            self.payload,
            Some(stream_search_response::Payload::ReadReady(_))
        )
    }
}

/// Used after admission, or immediately on a legacy stream. Neither allows
/// an unsolicited or duplicate receipt to disappear into a wildcard arm.
pub(crate) async fn next<M: ScanReadFrame, S>(stream: &mut S) -> Result<Option<M>, Status>
where
    S: tokio_stream::Stream<Item = Result<M, Status>> + Unpin,
{
    use tokio_stream::StreamExt;
    let message = stream.next().await.transpose()?;
    if message.as_ref().is_some_and(ScanReadFrame::is_receipt) {
        return Err(Status::failed_precondition(
            "duplicate or unsolicited vector ReadReady",
        ));
    }
    Ok(message)
}

struct Receipts {
    seen: Vec<bool>,
    binding: Option<Option<MappedVectorBinding>>,
    known: Vec<bool>,
}

pub(crate) struct VectorReadBarrier {
    field: String,
    visibility: Option<DocumentVisibility>,
    scope: VisibilityScope,
    claims: Vec<StatsClaim>,
    receipts: Mutex<Receipts>,
    ready: watch::Sender<Option<Result<(), Status>>>,
}
impl VectorReadBarrier {
    pub(crate) fn new(
        field: String,
        visibility: Option<DocumentVisibility>,
        claims: Vec<StatsClaim>,
    ) -> Result<Arc<Self>, Status> {
        if claims.is_empty() {
            return Err(Status::failed_precondition(
                "vector read has no admitted shards",
            ));
        }
        for claim in &claims {
            StatsClaim::required(claim.epoch, &claim.incarnation())?;
        }
        let scope = VisibilityScope::new(visibility.as_ref())?;
        let receipts = Receipts {
            seen: vec![false; claims.len()],
            binding: None,
            known: vec![false; scope.column_count()],
        };
        Ok(Arc::new(Self {
            field,
            visibility,
            scope,
            claims,
            receipts: Mutex::new(receipts),
            ready: watch::channel(None).0,
        }))
    }
    pub(crate) async fn admit<M: ScanReadFrame, S>(
        &self,
        shard: usize,
        stream: &mut S,
    ) -> Result<(), Status>
    where
        S: tokio_stream::Stream<Item = Result<M, Status>> + Unpin,
    {
        use tokio_stream::StreamExt;
        let result = async {
            let first = stream.next().await.transpose()?.ok_or_else(|| {
                Status::failed_precondition("vector stream closed before ReadReady")
            })?;
            self.accept(shard, &first.receipt()?)?;
            self.wait().await
        }
        .await;
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }
    pub(crate) fn context(&self, shard: usize) -> Result<VectorReadContext, Status> {
        let claim = self
            .claims
            .get(shard)
            .ok_or_else(|| Status::internal("vector read shard is outside admission"))?;
        Ok(VectorReadContext {
            field: self.field.clone(),
            visibility: self.visibility.clone(),
            expected_stats_epoch: claim.epoch,
            expected_stats_incarnation: claim.incarnation(),
        })
    }
    pub(crate) fn fail(&self, error: Status) {
        self.ready.send_if_modified(|state| {
            if matches!(state, Some(Err(_))) {
                false
            } else {
                *state = Some(Err(error.clone()));
                true
            }
        });
    }
    pub(crate) fn accept(&self, shard: usize, receipt: &VectorReadReceipt) -> Result<(), Status> {
        let result = (|| {
            let mut held = self
                .receipts
                .lock()
                .map_err(|_| Status::internal("vector receipt lock poisoned"))?;
            if held.seen.get(shard) != Some(&false) {
                return Err(Status::failed_precondition(
                    "duplicate or unrequested vector read receipt",
                ));
            }
            self.scope.validate_echo(
                &receipt.visibility_fingerprint,
                &receipt.visibility_columns_known,
            )?;
            let claim = StatsClaim::required(receipt.stats_epoch, &receipt.stats_incarnation)?;
            if self.claims.get(shard) != Some(&claim) {
                return Err(Status::failed_precondition(
                    "vector scan does not match its admitted physical version",
                ));
            }
            check_binding(
                &self.field,
                receipt.vector_binding.as_ref(),
                &mut held.binding,
            )?;
            for (known, present) in held.known.iter_mut().zip(&receipt.visibility_columns_known) {
                *known |= present;
            }
            held.seen[shard] = true;
            if held.seen.iter().all(|seen| *seen) {
                if held.known.iter().any(|known| !known) {
                    return Err(Status::failed_precondition(
                        "document grant references a column unavailable in this collection",
                    ));
                }
                // A failure on another stream remains terminal, even when
                // this last valid receipt arrives concurrently with it.
                self.ready.send_if_modified(|state| {
                    if state.is_none() {
                        *state = Some(Ok(()));
                        true
                    } else {
                        false
                    }
                });
            }
            Ok(())
        })();
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }
    pub(crate) async fn wait(&self) -> Result<(), Status> {
        let mut ready = self.ready.subscribe();
        loop {
            if let Some(result) = ready.borrow_and_update().clone() {
                return result;
            }
            ready
                .changed()
                .await
                .map_err(|_| Status::cancelled("vector admission closed"))?;
        }
    }
}

#[cfg(test)]
mod tests;
