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
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    fn setup() -> (Arc<VectorReadBarrier>, Vec<VectorReadReceipt>) {
        let binding = crate::mapping::derive_plan(
            include_bytes!("../tests/fixtures/vector-binding/descriptor.bin"),
            "vector_binding.Named",
        )
        .unwrap()
        .vector_binding
        .unwrap();
        let view = Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        });
        let scope = VisibilityScope::new(view.as_ref()).unwrap();
        let claims: Vec<_> = (1..=2)
            .map(|n| StatsClaim::required(4, &[n; 32]).unwrap())
            .collect();
        let receipts = claims
            .iter()
            .map(|claim| VectorReadReceipt {
                vector_binding: Some(binding.clone()),
                stats_epoch: claim.epoch,
                stats_incarnation: claim.incarnation(),
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: vec![true],
            })
            .collect();
        (
            VectorReadBarrier::new("semantic".into(), view, claims).unwrap(),
            receipts,
        )
    }
    fn frame(receipt: VectorReadReceipt) -> StreamSearchResponse {
        StreamSearchResponse {
            payload: Some(stream_search_response::Payload::ReadReady(receipt)),
        }
    }

    #[tokio::test]
    async fn fast_peer_stays_under_backpressure_until_the_whole_read_set_is_admitted() {
        let (barrier, receipts) = setup();
        let (tx, rx) = mpsc::channel(1);
        let consumer = barrier.clone();
        let task = tokio::spawn(async move {
            let mut stream = ReceiverStream::new(rx);
            consumer.admit(0, &mut stream).await?;
            next(&mut stream).await
        });
        tx.send(Ok(frame(receipts[0].clone()))).await.unwrap();
        let batch = StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Batch(StreamSearchBatch {
                hits: vec![7; 12],
            })),
        };
        tx.send(Ok(batch.clone())).await.unwrap();
        // The receipt has been consumed (capacity is one), but no candidate
        // may pass while the other peer is absent. Bounded buffering stays real.
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(30),
            tx.send(Ok(batch.clone()))
        )
        .await
        .is_err());
        barrier
            .admit(1, &mut tokio_stream::iter([Ok(frame(receipts[1].clone()))]))
            .await
            .unwrap();
        let found = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(found, batch);
    }

    #[tokio::test]
    async fn incompatible_or_missing_receipts_cancel_waiting_peers_before_data() {
        for case in 0..10 {
            let (barrier, mut receipts) = setup();
            let consumer = barrier.clone();
            let first = receipts[0].clone();
            let task = tokio::spawn(async move {
                consumer
                    .admit(0, &mut tokio_stream::iter([Ok(frame(first))]))
                    .await
            });
            let bad = &mut receipts[1];
            match case {
                0 => bad.vector_binding = None,
                1 => bad.vector_binding.as_mut().unwrap().field = "signal".into(),
                2 => bad.vector_binding.as_mut().unwrap().plan_fingerprint = "b".repeat(64),
                3 => bad.stats_epoch += 1,
                4 => bad.stats_incarnation.clear(),
                5 => bad.stats_incarnation = vec![1; 32],
                6 => bad.visibility_fingerprint.clear(),
                7 => bad.visibility_fingerprint = vec![9; 32],
                8 => bad.visibility_columns_known.clear(),
                _ => bad.vector_binding.as_mut().unwrap().version += 1,
            }
            assert!(barrier
                .admit(1, &mut tokio_stream::iter([Ok(frame(receipts[1].clone()))]))
                .await
                .is_err());
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn no_floor_candidate_or_completion_can_substitute_for_read_ready() {
        for frame in [
            SearchShardResponse { payload: None },
            SearchShardResponse {
                payload: Some(search_shard_response::Payload::FloorUpdate(FloorUpdate {
                    floor: 10.0,
                })),
            },
            SearchShardResponse {
                payload: Some(search_shard_response::Payload::Done(
                    SearchShardDone::default(),
                )),
            },
        ] {
            let (barrier, _) = setup();
            assert!(barrier
                .admit(0, &mut tokio_stream::iter([Ok(frame)]))
                .await
                .is_err());
            assert_eq!(barrier.receipts.lock().unwrap().seen, vec![false, false]);
        }
        for frame in [
            StreamSearchResponse { payload: None },
            StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Batch(StreamSearchBatch {
                    hits: vec![0; 12],
                })),
            },
            StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(
                    StreamSearchSummary {
                        completed: true,
                        ..Default::default()
                    },
                )),
            },
            StreamSearchResponse {
                payload: Some(stream_search_response::Payload::IdentityReady(
                    StreamIdentityReady::default(),
                )),
            },
            StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Identities(
                    StreamIdentities::default(),
                )),
            },
        ] {
            let (barrier, _) = setup();
            assert!(barrier
                .admit(0, &mut tokio_stream::iter([Ok(frame)]))
                .await
                .is_err());
            assert_eq!(barrier.receipts.lock().unwrap().seen, vec![false, false]);
        }
        let (barrier, _) = setup();
        assert!(barrier
            .admit(
                0,
                &mut tokio_stream::empty::<Result<StreamSearchResponse, Status>>()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duplicates_unknown_authority_columns_and_transport_errors_stay_terminal() {
        let (barrier, receipts) = setup();
        barrier.accept(0, &receipts[0]).unwrap();
        assert!(barrier.accept(0, &receipts[0]).is_err());
        barrier.accept(1, &receipts[1]).unwrap();
        assert!(barrier.wait().await.is_err()); // a late valid peer cannot reopen failure
        assert!(
            next(&mut tokio_stream::iter([Ok(frame(receipts[0].clone()))]))
                .await
                .is_err()
        );
        assert!(next(&mut tokio_stream::iter([Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::ReadReady(
                receipts[0].clone()
            )),
        })]))
        .await
        .is_err());
        let (barrier, mut receipts) = setup();
        for receipt in &mut receipts {
            receipt.visibility_columns_known = vec![false];
        }
        barrier.accept(0, &receipts[0]).unwrap();
        assert!(barrier.accept(1, &receipts[1]).is_err());
        assert!(barrier.wait().await.is_err());
        let (barrier, receipts) = setup();
        barrier.accept(0, &receipts[0]).unwrap();
        assert!(barrier
            .admit(
                1,
                &mut tokio_stream::iter([Err::<StreamSearchResponse, _>(Status::unavailable(
                    "peer lost"
                ))])
            )
            .await
            .is_err());
        assert_eq!(
            barrier.wait().await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }
}
