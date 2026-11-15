//! The child side of a relayed `Bm25QueryStream`: one outbound leg per
//! child and the reader tasks feeding their answers into one lane.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status};

use crate::pb::{bm25_query_stream_request, Bm25QueryStreamRequest, FloorUpdate, StopBm25Query};

use super::bm25_stream::{Bm25Args, ChildItem};
use super::stream_search::child_error;

/// The relay's outbound leg to each child and the reader task feeding
/// each child's answers into the shared lane. Dropping it stops the
/// readers.
pub(super) struct ChildLegs {
    pub(super) children: Vec<String>,
    pub(super) legs: Vec<Option<mpsc::Sender<Bm25QueryStreamRequest>>>,
    readers: Vec<JoinHandle<()>>,
}

impl Drop for ChildLegs {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.abort();
        }
    }
}

fn leg_frame(payload: bm25_query_stream_request::Payload) -> Bm25QueryStreamRequest {
    Bm25QueryStreamRequest {
        payload: Some(payload),
    }
}

impl ChildLegs {
    /// One stream per child, each started with the parent's query and
    /// that child's claim.
    pub(super) async fn open(
        args: &Bm25Args,
        events: mpsc::Sender<ChildItem>,
    ) -> Result<Self, Status> {
        let children = args.frozen.node_addresses().to_vec();
        let mut legs = Vec::with_capacity(children.len());
        let mut readers = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let link = args.frozen.node_client(addr)?;
            let (out_tx, out_rx) = mpsc::channel::<Bm25QueryStreamRequest>(8);
            let mut child_req = args.req.clone();
            child_req.expected_stats_epoch = args.claims[shard].epoch;
            child_req.expected_stats_incarnation = args.claims[shard].incarnation();
            out_tx
                .send(leg_frame(bm25_query_stream_request::Payload::Start(
                    child_req,
                )))
                .await
                .map_err(|_| Status::internal("relay: child request channel closed at start"))?;
            let mut request = Request::new(ReceiverStream::new(out_rx));
            if let Some(timeout) = args.timeout {
                request.set_timeout(timeout);
            }
            readers.push(spawn_reader(
                link,
                request,
                shard,
                addr.clone(),
                events.clone(),
            ));
            legs.push(Some(out_tx));
        }
        Ok(ChildLegs {
            children,
            legs,
            readers,
        })
    }

    /// Tell every child still open to stop, and close their legs.
    pub(super) async fn stop(&mut self) {
        let stops: Vec<_> = self.legs.iter_mut().filter_map(Option::take).collect();
        for leg in stops {
            let _ = leg
                .send(leg_frame(bm25_query_stream_request::Payload::Stop(
                    StopBm25Query {},
                )))
                .await;
        }
    }

    /// Forward a parent floor raise to every child still open.
    pub(super) async fn raise(&self, floor: f32) {
        for leg in self.legs.iter().flatten() {
            let _ = leg
                .send(leg_frame(bm25_query_stream_request::Payload::FloorUpdate(
                    FloorUpdate { floor },
                )))
                .await;
        }
    }
}

fn spawn_reader(
    mut link: crate::link::NodeLink,
    request: Request<ReceiverStream<Bm25QueryStreamRequest>>,
    shard: usize,
    addr: String,
    events: mpsc::Sender<ChildItem>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = match link.bm25_query_stream(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let _ = events
                    .send((shard, Err(child_error(shard, &addr, "bm25 stream", status))))
                    .await;
                return;
            }
        };
        loop {
            let next = stream.message().await;
            let done = matches!(next, Ok(None) | Err(_));
            let item = next.map_err(|status| child_error(shard, &addr, "bm25 stream", status));
            if events.send((shard, item)).await.is_err() || done {
                return;
            }
        }
    })
}
