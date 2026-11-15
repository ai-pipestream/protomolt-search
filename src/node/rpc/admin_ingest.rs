//! WAL, compaction, health, vector backend, snapshot, and document write RPCs.

use crate::node::*;

const STABLE_ROUTING_KEY_METADATA: &str = "x-protomolt-stable-key-bin";

fn replication_stable_key<T>(request: &Request<T>) -> Result<Option<Vec<u8>>, Status> {
    request
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
        .transpose()
}

impl NodeServiceImpl {
    pub(super) async fn handle_read_wal(
        &self,
        request: Request<ReadWalRequest>,
    ) -> Result<Response<<Self as NodeService>::ReadWalStream>, Status> {
        crate::metrics::timed_stream(Route::ReadWal, request, |request| async move {
            let req = request.into_inner();
            let (generation, high_watermark, records, prefix_health) = loop {
                let needs_flush = self
                    .state
                    .read()
                    .expect("shard state lock poisoned")
                    .wal
                    .as_ref()
                    .is_some_and(WalWriter::is_dirty);
                if needs_flush {
                    let service = self.clone();
                    tokio::task::spawn_blocking(move || service.flush_index())
                        .await
                        .map_err(|error| {
                            Status::internal(format!("WAL flush task failed: {error}"))
                        })??;
                }
                let snapshot = {
                    let guard = read_shard(&self.state);
                    let wal = guard.wal.as_ref().ok_or_else(|| {
                        Status::failed_precondition(
                            "this shard has no WAL; live catch-up is unavailable",
                        )
                    })?;
                    if req.generation != wal.generation() {
                        return Err(Status::failed_precondition(format!(
                            "WAL generation mismatch: requested {}, live {}",
                            req.generation,
                            wal.generation()
                        )));
                    }
                    if req.after_clock > wal.high_watermark() {
                        return Err(Status::invalid_argument(format!(
                            "after_clock {} is beyond WAL high watermark {}",
                            req.after_clock,
                            wal.high_watermark()
                        )));
                    }
                    let records =
                        wal::read_clocked_records(wal.dir(), req.after_clock).map_err(|error| {
                            Status::failed_precondition(format!("read WAL: {error}"))
                        })?;
                    let num_vectors = guard.index.as_ref().map_or(0, |index| index.len() as u64);
                    let document_slots = guard
                        .bm25
                        .as_ref()
                        .map_or(0, |shard| u64::from(shard.next_doc_id()));
                    let physical_docs = physical_rows(&guard);
                    let deleted_docs = guard.live_docs.deleted_count().min(physical_docs);
                    let scoring_fingerprint = guard
                        .index
                        .as_ref()
                        .map_or_else(String::new, |index| index.descriptor().scoring_fingerprint);
                    (
                        wal.generation(),
                        wal.high_watermark(),
                        records,
                        (
                            num_vectors,
                            document_slots,
                            physical_docs - deleted_docs,
                            deleted_docs,
                            scoring_fingerprint,
                        ),
                    )
                };
                // A writer can append after the dirty check and before the read
                // lock. Its buffered bytes then trail the in-memory watermark.
                // Never advertise that watermark: retry, which sees dirty=true
                // and makes the whole index/WAL prefix durable first.
                if snapshot.1 == req.after_clock
                    || snapshot
                        .2
                        .last()
                        .is_some_and(|record| record.clock == snapshot.1)
                {
                    break snapshot;
                }
                tokio::task::yield_now().await;
            };
            let (tx, rx) = mpsc::channel(64);
            let slot_offset = self.config.slot_offset;
            tokio::spawn(async move {
                for record in records {
                    if tx
                        .send(Ok(ReadWalResponse {
                            generation,
                            high_watermark,
                            record: prost::Message::encode_to_vec(&record),
                            completed: false,
                            ..Default::default()
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = tx
                    .send(Ok(ReadWalResponse {
                        generation,
                        high_watermark,
                        record: Vec::new(),
                        completed: true,
                        num_vectors: prefix_health.0,
                        document_slots: prefix_health.1,
                        live_docs: prefix_health.2,
                        deleted_docs: prefix_health.3,
                        scoring_fingerprint: prefix_health.4,
                        slot_offset,
                    }))
                    .await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_apply_wal_binding(
        &self,
        request: Request<crate::pb::ApplyWalBindingRequest>,
    ) -> Result<Response<crate::pb::ApplyWalBindingResponse>, Status> {
        let req = request.into_inner();
        self.admit_collection(&req.collection)?;
        if req.plan_fingerprint.is_empty() || req.body_path.is_empty() {
            return Err(Status::invalid_argument(
                "WAL binding requires plan_fingerprint and body_path",
            ));
        }
        let incoming = crate::postings::StoredBinding {
            plan_fingerprint: req.plan_fingerprint,
            body_path: req.body_path,
            materialize_sha: req.materialize_sha,
            analysis_sha: req.analysis_sha,
            analysis_contract: req.analysis_contract,
            vector_binding: req.vector_binding,
            index_contract: req.index_contract,
        };
        let _mutation = self.mutation_gate.read().await;
        let mut guard = write_shard(&self.state);
        let analysis_sha = incoming.analysis_sha.clone();
        let vector_binding = incoming.vector_binding.clone();
        let index_contract = incoming.index_contract.clone();
        if let Some(vector) =
            crate::mapped_vector::decode(&vector_binding, &incoming.plan_fingerprint)?
        {
            self.validate_vector_column_name(&vector.field, guard.bm25.as_ref())?;
        }
        let already_bound = Self::apply_binding_locked(&mut guard, incoming)?;
        Ok(Response::new(crate::pb::ApplyWalBindingResponse {
            already_bound,
            analysis_sha,
            vector_binding,
            index_contract,
        }))
    }

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
        let (bm25_docs, bm25_building) = match guard.bm25.as_ref() {
            Some(shard) => (shard.doc_count(), matches!(shard, Bm25Shard::Spilling(_))),
            None => (0, false),
        };
        let document_slots = guard
            .bm25
            .as_ref()
            .map_or(0, |shard| u64::from(shard.next_doc_id()));
        let exact_vector_rows = guard.exact_vectors.as_ref().map_or(0, |s| s.len() as u64);
        let exact_vectors_available = guard.exact_vectors.as_ref().is_some_and(|exact| {
            if let Some(index) = guard.index.as_ref() {
                exact.len() == index.len() && exact.dim() == index.dim_opt()
            } else if let Some(store) = guard.bm25.as_ref() {
                exact.len() == store.next_doc_id() as usize
            } else {
                true
            }
        });
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
            bm25_building,
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
                let bm25_view = guard.bm25.as_ref().and_then(|s| s.field_view(0));
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

    pub(super) async fn handle_install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "shard has no persistence path (index_path); a snapshot install IS persistence",
            )
        })?;
        let tmp_dir = generation_tmp_dir(&path);

        let mut inbound = request.into_inner();
        // Protocol: the first message must be the manifest.
        let manifest = match inbound.message().await? {
            Some(SnapshotChunk {
                payload: Some(snapshot_chunk::Payload::Manifest(m)),
            }) if m.vector_bytes > 0 => m,
            _ => {
                return Err(Status::invalid_argument(
                    "first SnapshotChunk must be a SnapshotManifest with vector_bytes > 0",
                ))
            }
        };

        if let Err(e) = Self::receive_image(&mut inbound, &manifest, &tmp_dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            return Err(e);
        }

        let service = self.clone();
        let cleanup = tmp_dir.clone();
        let with_exact_vectors = manifest.exact_vector_bytes > 0;
        let with_bm25 = manifest.bm25_bytes > 0;
        let with_live_docs = manifest.live_docs_bytes > 0;
        let result = tokio::task::spawn_blocking(move || {
            service.apply_snapshot(&tmp_dir, with_exact_vectors, with_bm25, with_live_docs)
        })
        .await
        .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
        if result.is_err() {
            // Rejected AFTER receive (bad image, calibration mismatch):
            // leave no staging dir behind either.
            let _ = tokio::fs::remove_dir_all(&cleanup).await;
        }
        result.map(Response::new)
    }

    pub(super) async fn handle_export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<ExportSnapshotResponse>, Status> {
        crate::metrics::timed(Route::ExportSnapshot, request, |request| async move {
            let directory = PathBuf::from(request.into_inner().directory);
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&directory))
                .await
                .map_err(|e| Status::internal(format!("export task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<<Self as NodeService>::StreamSnapshotStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSnapshot, request, |_request| async move {
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot export needs one",
                )
            })?;
            let staging = export_staging_dir(&path);
            let service = self.clone();
            let dir = staging.clone();
            let exported =
                tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&dir))
                    .await
                    .map_err(|e| Status::internal(format!("export task failed: {e}")))?;
            let exported = match exported {
                Ok(exported) => exported,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&staging).await;
                    return Err(e);
                }
            };
            let manifest = exported
                .manifest
                .ok_or_else(|| Status::internal("export produced no manifest"))?;
            let artifacts: Vec<PathBuf> = manifest
                .artifacts
                .iter()
                .map(|artifact| staging.join(&artifact.file))
                .collect();
            let (tx, rx) = mpsc::channel::<Result<SnapshotChunk, Status>>(2);
            tokio::spawn(async move {
                let first = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Repository(manifest)),
                };
                if tx.send(Ok(first)).await.is_ok() {
                    for artifact in artifacts {
                        if !stream_file(&tx, &artifact).await {
                            break;
                        }
                    }
                }
                let _ = tokio::fs::remove_dir_all(&staging).await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_install_snapshot_from(
        &self,
        request: Request<InstallSnapshotFromRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        crate::metrics::timed(Route::InstallSnapshotFrom, request, |request| async move {
            use crate::pb::install_snapshot_from_request::Source;
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot install IS persistence",
                )
            })?;
            let req = request.into_inner();
            let source = req.source.ok_or_else(|| {
                Status::invalid_argument(
                    "InstallSnapshotFrom needs a source: directory, url, or peer_addr",
                )
            })?;
            let tmp_dir = generation_tmp_dir(&path);
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            let staged = match source {
            Source::Directory(directory) => {
                let staging = tmp_dir.clone();
                tokio::task::spawn_blocking(move || {
                    Self::stage_from_directory(Path::new(&directory), &staging)
                })
                .await
                .map_err(|e| Status::internal(format!("stage task failed: {e}")))?
            }
            #[cfg(feature = "net")]
            Source::Url(url) => {
                crate::snapshot::stage_from_url(&url, &req.bearer_token, &tmp_dir).await
            }
            #[cfg(feature = "net")]
            Source::PeerAddr(peer) => crate::snapshot::stage_from_peer(&peer, &tmp_dir).await,
            #[cfg(not(feature = "net"))]
            Source::Url(_) | Source::PeerAddr(_) => Err(Status::failed_precondition(
                "this build has no network stack (feature `net` is off); install from a directory",
            )),
        };
            let (manifest, sha) = match staged {
                Ok(staged) => staged,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                    return Err(e);
                }
            };
            if let Err(e) = repo::check_expected_sha(&req.expected_manifest_sha256, &sha) {
                let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                return Err(Status::invalid_argument(e));
            }
            let service = self.clone();
            let cleanup = tmp_dir.clone();
            let result = tokio::task::spawn_blocking(move || {
                service.install_staged_repository(&tmp_dir, &manifest)
            })
            .await
            .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
            if result.is_err() {
                let _ = tokio::fs::remove_dir_all(&cleanup).await;
            }
            result.map(Response::new)
        })
        .await
    }

    async fn open_analysis_stream_or_refuse(
        &self,
        addr: &str,
        doc: &AddDocumentsRequest,
    ) -> Result<crate::analyzer::AnalyzeStream, Status> {
        // Refuse sidecars lacking AnalyzeStream explicitly rather than
        // falling back to unary per-document RPCs.
        match crate::analyzer::AnalyzeStream::open_with_vocab(
            addr,
            doc.analysis.as_ref(),
            self.vocab.clone(),
            self.doc_session_layers(doc),
        )
        .await
        {
            Ok(session) => Ok(session),
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                Err(Status::failed_precondition(format!(
                    "analysis server at {addr} does not implement AnalyzeStream; \
                     rebuild turbo-kserve from main. Refusing to ingest on the removed \
                     unary path."
                )))
            }
            Err(status) => Err(status),
        }
    }

    async fn run_ingest_source<'a>(
        &self,
        addr: &str,
        source: &mut IngestSource<'a>,
    ) -> Result<(u64, u64, u64, u64), Status> {
        let mut added = 0u64;
        let mut first_id = 0u64;
        // One analysis stream covers the whole call; documents apply in
        // arrival order so ids and WAL order stay deterministic.
        if let Some(first) = source.next().await? {
            let session = self
                .open_analysis_stream_or_refuse(addr, &first.req)
                .await?;
            self.ingest_streamed(session, first, source, addr, &mut added, &mut first_id)
                .await?;
        }
        let guard = read_shard(&self.state);
        let total = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let wal_generation = guard.wal.as_ref().map_or(0, WalWriter::generation);
        Ok((added, first_id, total, wal_generation))
    }

    pub(super) async fn handle_add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        crate::metrics::timed(Route::AddDocuments, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let addr = self.config.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable("no analysis backend configured for this shard (analysis_addr)")
            })?;
            let stable_routing_key = replication_stable_key(&request)?;
            let mut inbound = request.into_inner();
            let mut source = IngestSource::Plain {
                stream: &mut inbound,
                stable_routing_key,
                consumed: false,
            };
            let (added, first_id, total, wal_generation) =
                self.run_ingest_source(&addr, &mut source).await?;
            crate::metrics::add_ingested(added, 0);
            self.seal_if_due().await?;
            Ok(Response::new(AddDocumentsResponse {
                document_contract_version: crate::document_contract::VERSION,
                added,
                total,
                first_id,
                wal_generation,
            }))
        })
        .await
    }

    pub(super) async fn handle_delete_documents(
        &self,
        request: Request<DeleteDocumentsRequest>,
    ) -> Result<Response<DeleteDocumentsResponse>, Status> {
        crate::metrics::timed(Route::DeleteDocuments, request, |request| async move {
            let req = request.into_inner();
            let _mutation = self.mutation_gate.read().await;
            let mut guard = write_shard(&self.state);
            self.delete_documents_locked(&mut guard, &req.doc_ids, req.expected_wal_generation)
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_commit_replacements(
        &self,
        request: Request<CommitReplacementsRequest>,
    ) -> Result<Response<CommitReplacementsResponse>, Status> {
        crate::metrics::timed(Route::CommitReplacements, request, |request| async move {
            let req = request.into_inner();
            let _mutation = self.mutation_gate.read().await;
            let mut guard = write_shard(&self.state);
            self.commit_replacements_locked(
                &mut guard,
                &req.replacements,
                req.expected_wal_generation,
            )
            .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_ingest_mapped(
        &self,
        request: Request<Streaming<IngestMappedRequest>>,
    ) -> Result<Response<IngestMappedResponse>, Status> {
        crate::metrics::timed(Route::IngestMapped, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let addr = self.config.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable("no analysis backend configured for this shard (analysis_addr)")
            })?;
            let mut inbound = request.into_inner();
            // Protocol: the first message must be the bind, and the bind
            // must stand — fingerprint agreement, declared columns, a body
            // — before a single document streams.
            let bind = match inbound.message().await? {
                Some(IngestMappedRequest {
                    payload: Some(crate::pb::ingest_mapped_request::Payload::Bind(bind)),
                }) => bind,
                _ => {
                    return Err(Status::invalid_argument(
                        "first IngestMappedRequest must be a MappedBind",
                    ))
                }
            };
            self.admit_collection(&bind.collection)?;
            let (extractor, analysis) = {
                let _mutation = self.mutation_gate.read().await;
                self.bind_mapped(&bind)?
            };
            let fingerprint = extractor.plan().fingerprint.clone();
            let mut source = IngestSource::Mapped(Box::new(MappedSource {
                stream: &mut inbound,
                extractor,
                analysis: analysis.body,
                field_analysis: analysis.fields,
                materialize: bind.materialize.clone(),
                position: 0,
                parents: 0,
                rows: std::collections::VecDeque::new(),
                original_source: crate::pb::ProtobufSource {
                    descriptor_set: bind.descriptor_set.clone(),
                    message_type: bind.message_type.clone(),
                    payload: Vec::new(),
                },
            }));
            let (added, first_id, total, wal_generation) =
                self.run_ingest_source(&addr, &mut source).await?;
            // Each mapped row carries exactly one vector.
            crate::metrics::add_ingested(added, added);
            let parents = match &source {
                IngestSource::Mapped(mapped) => mapped.parents,
                IngestSource::Plain { .. } | IngestSource::Prepared(_) => {
                    unreachable!("this handler built a mapped source")
                }
            };
            Ok(Response::new(IngestMappedResponse {
                added,
                total,
                first_id,
                fingerprint,
                parents,
                wal_generation,
            }))
        })
        .await
    }
}
