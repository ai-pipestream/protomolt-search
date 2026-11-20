//! Compaction, health, vector backend, and vector write RPCs.

mod documents;
mod snapshot;
mod wal_feed;

use crate::node::*;

const STABLE_ROUTING_KEY_METADATA: &str = "x-protomolt-stable-key-bin";

fn replication_stable_key<T>(request: &Request<T>) -> Result<Option<Vec<u8>>, Status> {
    let key = request
        .metadata()
        .get_bin(STABLE_ROUTING_KEY_METADATA)
        .map(|value| {
            value
                .to_bytes()
                .map(|bytes| bytes.to_vec())
                .map_err(|error| {
                    Status::invalid_argument(format!(
                        "invalid replication stable-key metadata: {error}"
                    ))
                })
        })
        .transpose()?;
    if key.as_deref().is_some_and(<[u8]>::is_empty) {
        return Err(Status::invalid_argument(
            "replication stable-key metadata is present but empty; an empty key routes              nothing and proves no colocation",
        ));
    }
    Ok(key)
}

/// Whether the exact-vector sidecar covers every row the shard serves.
fn exact_vectors_available(guard: &ShardState) -> bool {
    guard.exact_vectors.as_ref().is_some_and(|exact| {
        if let Some(index) = guard.index.as_ref() {
            exact.len() == index.len() && exact.dim() == index.dim_opt()
        } else if let Some(store) = guard.bm25.as_ref() {
            exact.len() == store.next_doc_id() as usize
        } else {
            true
        }
    })
}

impl NodeServiceImpl {
    pub(super) async fn handle_compact_shard(
        &self,
        request: Request<crate::pb::CompactShardRequest>,
    ) -> Result<Response<crate::pb::CompactShardResponse>, Status> {
        crate::metrics::timed(Route::CompactShard, request, |request| async move {
            let req = request.into_inner();
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.compact_shard(&req))
                .await
                .map_err(|e| Status::internal(format!("compaction task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let guard = read_shard(&self.state);
        let (num_vectors, dim, bit_width, vector_backend, scoring_fingerprint, quality_contract) =
            match guard.index.as_ref() {
                Some(index) => {
                    let descriptor = index.descriptor();
                    (
                        index.len() as u64,
                        index.dim_opt().unwrap_or(0) as u32,
                        index.bits_per_dimension().unwrap_or(self.config.bit_width) as u32,
                        descriptor.backend_kind,
                        descriptor.scoring_fingerprint,
                        format!("{:?}", descriptor.quality_contract).to_ascii_lowercase(),
                    )
                }
                None => (
                    0,
                    0,
                    self.config.bit_width as u32,
                    self.config.vector_backend.clone(),
                    String::new(),
                    String::new(),
                ),
            };
        let bm25_docs = guard.bm25.as_ref().map_or(0, Bm25Shard::doc_count);
        let document_slots = guard
            .bm25
            .as_ref()
            .map_or(0, |shard| u64::from(shard.next_doc_id()));
        let exact_vector_rows = guard.exact_vectors.as_ref().map_or(0, |s| s.len() as u64);
        let exact_vectors_available = exact_vectors_available(&guard);
        let exact_vectors_mmap = guard
            .exact_vectors
            .as_ref()
            .is_some_and(ExactVectorStore::is_mapped);
        let physical_docs = physical_rows(&guard);
        let deleted_docs = guard.live_docs.deleted_count().min(physical_docs);
        let (wal_generation, wal_high_watermark, wal_clocked) =
            guard.wal.as_ref().map_or((0, 0, false), |wal| {
                (wal.generation(), wal.high_watermark(), true)
            });
        Ok(Response::new(HealthResponse {
            multi_res_vector_version: 1,
            document_contract_version: crate::document_contract::VERSION,
            collection: self.config.collection.clone(),
            num_vectors,
            dim,
            bits_per_dimension: bit_width,
            slot_offset: self.config.slot_offset,
            bm25_docs,
            bm25_building: false,
            ingest_active: self.ingest_busy.load(std::sync::atomic::Ordering::Acquire),
            vector_backend,
            scoring_fingerprint,
            quality_contract,
            exact_vectors_available,
            exact_vector_rows,
            exact_vectors_mmap,
            live_docs: physical_docs - deleted_docs,
            deleted_docs,
            live_revision: guard.live_docs.revision(),
            wal_generation,
            wal_high_watermark,
            wal_clocked,
            document_slots,
        }))
    }

    pub(super) async fn handle_get_vector_backend(
        &self,
        _request: Request<GetVectorBackendRequest>,
    ) -> Result<Response<GetVectorBackendResponse>, Status> {
        let guard = read_shard(&self.state);
        let Some(index) = guard.index.as_ref() else {
            return Ok(Response::new(GetVectorBackendResponse {
                descriptor: None,
                config: None,
                num_vectors: 0,
            }));
        };
        let config = index
            .backend_config()
            .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
        Ok(Response::new(GetVectorBackendResponse {
            descriptor: Some(wire_backend_descriptor(index)),
            config: Some(wire_backend_config(&config)),
            num_vectors: index.len() as u64,
        }))
    }

    pub(super) async fn handle_configure_vector_backend(
        &self,
        request: Request<ConfigureVectorBackendRequest>,
    ) -> Result<Response<ConfigureVectorBackendResponse>, Status> {
        let _mutation = self.mutation_gate.read().await;
        let already_configured = self.apply_backend_config(request.into_inner())?;
        Ok(Response::new(ConfigureVectorBackendResponse {
            already_configured,
        }))
    }

    pub(super) async fn handle_add_vectors(
        &self,
        request: Request<Streaming<AddVectorsRequest>>,
    ) -> Result<Response<AddVectorsResponse>, Status> {
        crate::metrics::timed(Route::AddVectors, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let stable_routing_key = replication_stable_key(&request)?;
            let mut inbound = request.into_inner();
            let mut added = 0u64;
            let mut first_id = 0u64;
            let mut batches = 0usize;
            while let Some(batch) = inbound.message().await? {
                batches += 1;
                if stable_routing_key.is_some() && batches > 1 {
                    return Err(Status::invalid_argument(
                    "a replication stable-key metadata value may carry exactly one vector batch",
                ));
                }
                let service = self.clone();
                let key = stable_routing_key.clone();
                let mutation = self.mutation_gate.clone().read_owned().await;
                let (batch_added, batch_first_id) = tokio::task::spawn_blocking(move || {
                    let _mutation = mutation;
                    service.apply_batch(batch, key)
                })
                .await
                .map_err(|e| Status::internal(format!("add task failed: {e}")))??;
                if added == 0 && batch_added > 0 {
                    first_id = batch_first_id;
                }
                added += batch_added;
                self.seal_if_due().await?;
            }
            let (total, wal_generation) = {
                let guard = read_shard(&self.state);
                (
                    guard.index.as_ref().map_or(0, |i| i.len() as u64),
                    guard.wal.as_ref().map_or(0, WalWriter::generation),
                )
            };
            crate::metrics::add_ingested(0, added);
            Ok(Response::new(AddVectorsResponse {
                added,
                total,
                first_id,
                wal_generation,
            }))
        })
        .await
    }

    pub(super) async fn handle_flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let service = self.clone();
        let mutation = self.mutation_gate.clone().read_owned().await;
        let resp = tokio::task::spawn_blocking(move || {
            let _mutation = mutation;
            service.flush_index()
        })
        .await
        .map_err(|e| Status::internal(format!("flush task failed: {e}")))??;
        #[cfg(feature = "net")]
        if let Some(endpoint) = self.config.vocab_kserve_endpoint.as_deref() {
            let export_batch = {
                let guard = read_shard(&self.state);
                let bm25_view = guard.bm25.as_ref().map(|s| s.field_view(0));
                let shard_docs = guard.bm25.as_ref().map_or(0, |s| s.doc_count());
                self.vocab.as_ref().and_then(|vocab| {
                    let stats = vocab.live_stats();
                    let terms_ch = stats
                        .into_iter()
                        .find(|c| c.channel == crate::pb::storage::VocabChannel::Terms as i32)?;
                    let doc_count = shard_docs.max(terms_ch.documents);
                    if doc_count == 0 || terms_ch.heavy_hitters.is_empty() {
                        return None;
                    }
                    let freqs: Vec<(String, u64)> = terms_ch
                        .heavy_hitters
                        .into_iter()
                        .filter_map(|hh| {
                            let df = bm25_view
                                .as_ref()
                                .map(|v| u64::from(v.df(&hh.term)))
                                .unwrap_or(hh.count)
                                .min(doc_count);
                            (df > 0).then_some((hh.term, df))
                        })
                        .collect();
                    (!freqs.is_empty()).then_some((doc_count, freqs))
                })
            };
            if let Some((doc_count, freqs)) = export_batch {
                let remote = crate::analyzer::remote_client_for(endpoint, &Default::default())?;
                crate::vocab::export_corpus_statistics_to_kserve(
                    &remote,
                    "eng",
                    "shard-vocab",
                    doc_count,
                    &freqs,
                    0,
                )
                .await?;
            }
        }
        Ok(Response::new(resp))
    }
}
