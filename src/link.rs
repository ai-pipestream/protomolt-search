//! The coordinator's link to one node (`docs/embedded-mobile.md`): the
//! generated gRPC client over a channel for a node reached across the
//! network, or a direct dispatch into a [`NodeServiceImpl`] living in
//! this process for the embedded runtime. Both go through the same
//! handlers — the `NodeService` trait implementation — so an embedded
//! query is the network query minus the wire, not a second ranking
//! path. The local dispatch speaks no HTTP/2: a client-streaming call
//! frames the caller's messages into the handler's `Streaming` body
//! in memory, and a server stream comes back as the handler's own
//! receiver. That is what lets the embedded crate link without `h2`,
//! `hyper`, or Tokio's networking.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::codec::{Codec, ProstCodec, Streaming};
use tonic::{IntoRequest, IntoStreamingRequest, Request, Response, Status};

use crate::node::NodeServiceImpl;
#[cfg(feature = "net")]
use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::node_service_server::NodeService;
use crate::pb::*;

/// One node as the coordinator reaches it.
#[derive(Clone)]
pub enum NodeLink {
    /// A node across the network, through tonic's channel.
    #[cfg(feature = "net")]
    Remote(NodeServiceClient<tonic::transport::Channel>),
    /// A node in this process, called directly.
    Local(Arc<NodeServiceImpl>),
}

impl std::fmt::Debug for NodeLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "net")]
            NodeLink::Remote(_) => f.write_str("NodeLink::Remote"),
            NodeLink::Local(_) => f.write_str("NodeLink::Local"),
        }
    }
}

/// A server stream from a node, whichever way the node is reached. It
/// is a [`Stream`] and also answers `message()` like tonic's, so the
/// coordinator reads both kinds the same way.
pub enum LinkStream<T> {
    #[cfg(feature = "net")]
    Remote(Box<Streaming<T>>),
    Local(crate::metrics::Timed<ReceiverStream<Result<T, Status>>>),
}

impl<T> LinkStream<T> {
    pub async fn message(&mut self) -> Result<Option<T>, Status> {
        use tokio_stream::StreamExt;
        match self {
            #[cfg(feature = "net")]
            LinkStream::Remote(stream) => stream.message().await,
            LinkStream::Local(stream) => match stream.next().await {
                Some(Ok(message)) => Ok(Some(message)),
                Some(Err(status)) => Err(status),
                None => Ok(None),
            },
        }
    }
}

impl<T> Stream for LinkStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            #[cfg(feature = "net")]
            LinkStream::Remote(stream) => Pin::new(stream.as_mut()).poll_next(cx),
            LinkStream::Local(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

/// The caller's request stream as the gRPC-framed body a handler's
/// `Streaming` decodes: a five-byte prefix (no compression, big-endian
/// length) and the protobuf bytes per message, produced as the caller's
/// stream yields. No socket, no HTTP/2 — the frames never leave memory.
struct FramedBody<S> {
    stream: Pin<Box<S>>,
}

impl<S, T> http_body::Body for FramedBody<S>
where
    S: Stream<Item = T> + Send + 'static,
    T: prost::Message,
{
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(message)) => {
                let len = message.encoded_len();
                let Ok(len_u32) = u32::try_from(len) else {
                    return Poll::Ready(Some(Err(Status::resource_exhausted(format!(
                        "an in-process request message of {len} bytes exceeds the gRPC frame"
                    )))));
                };
                let mut frame = BytesMut::with_capacity(5 + len);
                frame.put_u8(0);
                frame.put_u32(len_u32);
                message.encode(&mut frame).map_err(|error| {
                    Status::internal(format!("encode in-process request message: {error}"))
                })?;
                Poll::Ready(Some(Ok(http_body::Frame::data(frame.freeze()))))
            }
        }
    }
}

/// The caller's streaming request as the handler receives it: same
/// metadata and extensions, the messages decoded from an in-memory
/// framed body under the same size cap the network path applies.
fn local_stream<T, R>(request: R) -> Request<Streaming<T>>
where
    T: prost::Message + Default + Send + 'static,
    R: IntoStreamingRequest<Message = T>,
{
    let (metadata, extensions, stream) = request.into_streaming_request().into_parts();
    let body = FramedBody {
        stream: Box::pin(stream),
    };
    let decoder = ProstCodec::<T, T>::default().decoder();
    let streaming = Streaming::new_request(decoder, body, None, Some(crate::MAX_MESSAGE_BYTES));
    Request::from_parts(metadata, extensions, streaming)
}

macro_rules! unary {
    ($($name:ident: $req:ty => $resp:ty),* $(,)?) => {
        $(
            pub async fn $name(
                &mut self,
                request: impl IntoRequest<$req>,
            ) -> Result<Response<$resp>, Status> {
                match self {
                    #[cfg(feature = "net")]
                    NodeLink::Remote(client) => client.$name(request).await,
                    NodeLink::Local(node) => {
                        NodeService::$name(&**node, request.into_request()).await
                    }
                }
            }
        )*
    };
}

macro_rules! client_streaming {
    ($($name:ident: $req:ty => $resp:ty),* $(,)?) => {
        $(
            pub async fn $name(
                &mut self,
                request: impl IntoStreamingRequest<Message = $req>,
            ) -> Result<Response<$resp>, Status> {
                match self {
                    #[cfg(feature = "net")]
                    NodeLink::Remote(client) => client.$name(request).await,
                    NodeLink::Local(node) => {
                        NodeService::$name(&**node, local_stream(request)).await
                    }
                }
            }
        )*
    };
}

macro_rules! server_streaming {
    ($($name:ident: $req:ty => $resp:ty),* $(,)?) => {
        $(
            pub async fn $name(
                &mut self,
                request: impl IntoRequest<$req>,
            ) -> Result<Response<LinkStream<$resp>>, Status> {
                match self {
                    #[cfg(feature = "net")]
                    NodeLink::Remote(client) => {
                        client
                        .$name(request)
                        .await
                        .map(|r| r.map(|stream| LinkStream::Remote(Box::new(stream))))
                    }
                    NodeLink::Local(node) => NodeService::$name(&**node, request.into_request())
                        .await
                        .map(|r| r.map(LinkStream::Local)),
                }
            }
        )*
    };
}

macro_rules! bidi_streaming {
    ($($name:ident: $req:ty => $resp:ty),* $(,)?) => {
        $(
            pub async fn $name(
                &mut self,
                request: impl IntoStreamingRequest<Message = $req>,
            ) -> Result<Response<LinkStream<$resp>>, Status> {
                match self {
                    #[cfg(feature = "net")]
                    NodeLink::Remote(client) => {
                        client
                        .$name(request)
                        .await
                        .map(|r| r.map(|stream| LinkStream::Remote(Box::new(stream))))
                    }
                    NodeLink::Local(node) => NodeService::$name(&**node, local_stream(request))
                        .await
                        .map(|r| r.map(LinkStream::Local)),
                }
            }
        )*
    };
}

impl NodeLink {
    /// A node in this process.
    pub fn local(node: Arc<NodeServiceImpl>) -> Self {
        NodeLink::Local(node)
    }

    /// A node across the network, with the engine's message size caps.
    #[cfg(feature = "net")]
    pub fn remote(channel: tonic::transport::Channel) -> Self {
        NodeLink::Remote(
            NodeServiceClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(crate::MAX_MESSAGE_BYTES),
        )
    }

    pub fn is_local(&self) -> bool {
        matches!(self, NodeLink::Local(_))
    }

    unary! {
        get_vector_backend: GetVectorBackendRequest => GetVectorBackendResponse,
        configure_vector_backend: ConfigureVectorBackendRequest => ConfigureVectorBackendResponse,
        get_calibration: GetCalibrationRequest => GetCalibrationResponse,
        set_calibration: SetCalibrationRequest => SetCalibrationResponse,
        flush: FlushRequest => FlushResponse,
        delete_documents: DeleteDocumentsRequest => DeleteDocumentsResponse,
        commit_replacements: CommitReplacementsRequest => CommitReplacementsResponse,
        term_stats: TermStatsRequest => TermStatsResponse,
        expand_term_prefix: ExpandTermPrefixRequest => ExpandTermPrefixResponse,
        suggest_terms: SuggestTermsRequest => SuggestTermsResponse,
        bm25_query: Bm25QueryRequest => Bm25QueryResponse,
        bm25_phrase_query: Bm25PhraseQueryRequest => Bm25QueryResponse,
        get_documents: GetDocumentsRequest => GetDocumentsResponse,
        resolve_parents: ResolveParentsRequest => ResolveParentsResponse,
        bm25_rescore: Bm25RescoreRequest => Bm25RescoreResponse,
        fetch_values: FetchValuesRequest => FetchValuesResponse,
        aggregate_shard: AggregateShardRequest => AggregateShardResponse,
        quantile_counts: QuantileCountsRequest => QuantileCountsResponse,
        vector_rescore: VectorRescoreRequest => VectorRescoreResponse,
        exact_vector_rescore: ExactVectorRescoreRequest => ExactVectorRescoreResponse,
        hybrid_shard: HybridShardRequest => HybridShardResponse,
        shard_legs: ShardLegsRequest => ShardLegsResponse,
        browse_shard: BrowseShardRequest => BrowseShardResponse,
        resolve_filter_bitmap: FilterBitmapRequest => FilterBitmapResponse,
        resolve_lexical_bitmap: LexicalBitmapRequest => MembershipBitmapResponse,
        resolve_vector_bitmap: VectorBitmapRequest => MembershipBitmapResponse,
        evaluate_boolean: BooleanShardRequest => BooleanShardResponse,
        apply_wal_binding: ApplyWalBindingRequest => ApplyWalBindingResponse,
        health: HealthRequest => HealthResponse,
    }

    client_streaming! {
        add_vectors: AddVectorsRequest => AddVectorsResponse,
        install_snapshot: SnapshotChunk => InstallSnapshotResponse,
        add_documents: AddDocumentsRequest => AddDocumentsResponse,
        ingest_mapped: IngestMappedRequest => IngestMappedResponse,
    }

    server_streaming! {
        read_wal: ReadWalRequest => ReadWalResponse,
    }

    bidi_streaming! {
        search_shard: SearchShardRequest => SearchShardResponse,
        stream_search: StreamSearchRequest => StreamSearchResponse,
        bm25_query_stream: Bm25QueryStreamRequest => Bm25QueryStreamResponse,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    /// The in-memory framing decodes back to the same messages, metadata
    /// included, under the handler-side decoder.
    #[tokio::test]
    async fn framed_body_round_trips_messages_and_metadata() {
        let docs: Vec<AddDocumentsRequest> = (0..3)
            .map(|i| AddDocumentsRequest {
                text: format!("document {i} with some text"),
                ..Default::default()
            })
            .collect();
        let mut request = Request::new(tokio_stream::iter(docs.clone()));
        request
            .metadata_mut()
            .insert("x-stable-key", "opinion:1".parse().unwrap());
        let mut streaming = local_stream(request);
        assert_eq!(
            streaming.metadata().get("x-stable-key").unwrap(),
            "opinion:1"
        );
        let mut got = Vec::new();
        while let Some(message) = streaming.get_mut().next().await {
            got.push(message.unwrap());
        }
        assert_eq!(got, docs);
    }
}
