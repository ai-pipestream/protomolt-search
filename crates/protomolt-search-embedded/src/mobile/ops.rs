//! The byte-level operations behind every ABI entry point: decode the
//! request, run it on the embedded runtime, encode the envelope.

use std::sync::Arc;

use tokio_stream::StreamExt;

use pipestream_search::embedded::EmbeddedSearch;
use pipestream_search::pb::mobile::{
    MobileCloseResponse, MobileErrorCode, MobileFlushResponse, MobileIngestMappedBatch,
    MobileOpenRequest, MobileOpenResponse, MobileQueryStreamNextResponse,
    MobileQueryStreamOpenResponse,
};
use pipestream_search::pb::{
    AcceptDocumentRequest, DescribeSchemaRequest, PlanIndexRequest, QueryRequest, QueryResponse,
    QueryStreamRequest, ReadAcceptedDocumentsRequest,
};

use super::bridge::{decode, lock_registry, response, runtime, success, BridgeError, OpenStream};
use super::config::embedded_config;

pub(super) fn search(handle: u64) -> Result<Arc<EmbeddedSearch>, BridgeError> {
    lock_registry()?
        .searches
        .get(&handle)
        .cloned()
        .ok_or_else(|| BridgeError::not_found(format!("unknown search handle {handle}")))
}

pub(super) fn open_bytes(input: &[u8], create: bool) -> Vec<u8> {
    response(|| {
        let request = decode::<MobileOpenRequest>(input, "MobileOpenRequest")?;
        let config = embedded_config(request)?;
        let runtime = runtime()?;
        let search = if create {
            runtime.block_on(EmbeddedSearch::create(config))?
        } else {
            runtime.block_on(EmbeddedSearch::open(config))?
        };
        if search.allows_network() {
            return Err(BridgeError::internal(
                "embedded runtime unexpectedly permits network access",
            ));
        }
        let shard_count = search.shard_count() as u32;
        let mut registry = lock_registry()?;
        let handle = registry.allocate()?;
        registry.searches.insert(handle, Arc::new(search));
        Ok(success(&MobileOpenResponse {
            handle,
            shard_count,
            no_egress: true,
        }))
    })
}

pub(super) fn ingest_mapped_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let batch = decode::<MobileIngestMappedBatch>(input, "MobileIngestMappedBatch")?;
        let search = search(handle)?;
        let result =
            runtime()?.block_on(search.ingest_mapped(batch.shard as usize, batch.requests))?;
        Ok(success(&result))
    })
}

pub(super) fn accept_document_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<AcceptDocumentRequest>(input, "AcceptDocumentRequest")?;
        let result = search(handle)?.accept_document(&request)?;
        Ok(success(&result))
    })
}

pub(super) fn describe_schema_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<DescribeSchemaRequest>(input, "DescribeSchemaRequest")?;
        let search = search(handle)?;
        let result = runtime()?.block_on(search.describe_schema(request))?;
        Ok(success(&result))
    })
}

pub(super) fn plan_index_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<PlanIndexRequest>(input, "PlanIndexRequest")?;
        let search = search(handle)?;
        let result = runtime()?.block_on(search.plan_index(request))?;
        Ok(success(&result))
    })
}

pub(super) fn query_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryRequest>(input, "QueryRequest")?;
        let search = search(handle)?;
        let result: QueryResponse = runtime()?.block_on(search.query(request))?;
        Ok(success(&result))
    })
}

pub(super) fn read_accepted_documents_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request =
            decode::<ReadAcceptedDocumentsRequest>(input, "ReadAcceptedDocumentsRequest")?;
        let result = search(handle)?.read_accepted_documents(&request)?;
        Ok(success(&result))
    })
}

pub(super) fn query_stream_open_bytes(handle: u64, input: &[u8]) -> Vec<u8> {
    response(|| {
        let request = decode::<QueryStreamRequest>(input, "QueryStreamRequest")?;
        let search = search(handle)?;
        let receiver = runtime()?.block_on(search.query_stream(request))?;
        let mut registry = lock_registry()?;
        if !registry.searches.contains_key(&handle) {
            return Err(BridgeError::not_found(format!(
                "unknown search handle {handle}"
            )));
        }
        let stream_handle = registry.allocate()?;
        registry.streams.insert(
            stream_handle,
            OpenStream {
                owner: handle,
                receiver: Some(receiver),
                cancel: tokio::sync::watch::channel(false).0,
            },
        );
        Ok(success(&MobileQueryStreamOpenResponse { stream_handle }))
    })
}

pub(super) fn query_stream_next_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let (mut receiver, mut cancel) = {
            let mut registry = lock_registry()?;
            let open = registry.streams.get_mut(&stream_handle).ok_or_else(|| {
                BridgeError::not_found(format!("unknown stream handle {stream_handle}"))
            })?;
            let receiver = open.receiver.take().ok_or_else(|| BridgeError {
                code: MobileErrorCode::FailedPrecondition,
                message: "a read is already pending on this stream".into(),
            })?;
            (receiver, open.cancel.subscribe())
        };
        let item = runtime()?.block_on(async {
            tokio::select! {
                biased;
                _ = cancel.changed() => None,
                item = receiver.next() => Some(item),
            }
        });
        let mut registry = lock_registry()?;
        if !registry.streams.contains_key(&stream_handle) || item.is_none() {
            return Err(BridgeError {
                code: MobileErrorCode::Cancelled,
                message: "mobile query stream closed".into(),
            });
        }
        match item.unwrap() {
            Some(Ok(item)) => {
                registry.streams.get_mut(&stream_handle).unwrap().receiver = Some(receiver);
                Ok(success(&MobileQueryStreamNextResponse {
                    response: Some(item),
                    end: false,
                }))
            }
            Some(Err(status)) => {
                registry.streams.remove(&stream_handle);
                Err(status.into())
            }
            None => {
                registry.streams.remove(&stream_handle);
                Ok(success(&MobileQueryStreamNextResponse {
                    response: None,
                    end: true,
                }))
            }
        }
    })
}

pub(super) fn query_stream_close_bytes(stream_handle: u64) -> Vec<u8> {
    response(|| {
        let stream = lock_registry()?.streams.remove(&stream_handle);
        let closed = stream.is_some();
        if let Some(stream) = stream {
            let _ = stream.cancel.send(true);
        }
        Ok(success(&MobileCloseResponse { closed }))
    })
}

pub(super) fn flush_bytes(handle: u64) -> Vec<u8> {
    response(|| {
        let search = search(handle)?;
        let shards = runtime()?.block_on(search.flush_all())?;
        Ok(success(&MobileFlushResponse { shards }))
    })
}

pub(super) fn close_bytes(handle: u64) -> Vec<u8> {
    response(|| {
        let mut registry = lock_registry()?;
        let closed = registry.searches.remove(&handle).is_some();
        registry.streams.retain(|_, stream| {
            if stream.owner == handle {
                let _ = stream.cancel.send(true);
                false
            } else {
                true
            }
        });
        Ok(success(&MobileCloseResponse { closed }))
    })
}
