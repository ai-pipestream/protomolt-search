//! Per-shard response reader tasks feeding the merged inbound lane.

use super::*;

type StreamInboundSender = mpsc::Sender<(usize, Result<Option<StreamSearchResponse>, Status>)>;

pub(super) struct StreamReader {
    pub(super) shard: usize,
    pub(super) client: crate::link::NodeLink,
    pub(super) requests: mpsc::Receiver<StreamSearchRequest>,
    pub(super) merged: StreamInboundSender,
    pub(super) admission: Option<Arc<crate::vector_read::VectorReadBarrier>>,
    pub(super) deadline: Option<Duration>,
}

impl StreamReader {
    pub(super) async fn run(self) {
        let Self {
            shard,
            mut client,
            requests,
            merged,
            admission,
            deadline,
        } = self;
        let result = {
            let read =
                forward_stream_reader(&mut client, shard, requests, &merged, admission.as_deref());
            if let Some(deadline) = deadline {
                tokio::time::timeout(deadline, read)
                    .await
                    .unwrap_or_else(|_| {
                        Err(Status::deadline_exceeded(
                            "vector stream exceeded shard deadline",
                        ))
                    })
            } else {
                read.await
            }
        };
        if let Err(error) = result {
            if let Some(admission) = &admission {
                admission.fail(error.clone());
            }
            let _ = merged.send((shard, Err(error))).await;
        }
    }
}

async fn forward_stream_reader(
    client: &mut crate::link::NodeLink,
    shard: usize,
    requests: mpsc::Receiver<StreamSearchRequest>,
    merged: &StreamInboundSender,
    admission: Option<&crate::vector_read::VectorReadBarrier>,
) -> Result<(), Status> {
    let mut inbound = client
        .stream_search(Request::new(ReceiverStream::new(requests)))
        .await?
        .into_inner();
    if let Some(admission) = admission {
        admission.admit(shard, &mut inbound).await?;
    }
    loop {
        match crate::vector_read::next(&mut inbound).await {
            Ok(Some(message)) => {
                if merged.send((shard, Ok(Some(message)))).await.is_err() {
                    return Ok(());
                }
            }
            Ok(None) => {
                let _ = merged.send((shard, Ok(None))).await;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}
