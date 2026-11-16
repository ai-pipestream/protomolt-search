//! Document write RPCs: plain and mapped ingest, deletion, and replacement.

use crate::node::*;

impl NodeServiceImpl {
    /// Refuse a WAL replay stream that does not start at this shard's
    /// document tip. The caller holds the single-writer ingest slot, so the
    /// tip cannot move before the stream's first row applies.
    fn require_document_tip(&self, claimed: u64) -> Result<(), Status> {
        let guard = read_shard(&self.state);
        let tip = self.config.slot_offset
            + guard
                .bm25
                .as_ref()
                .map_or(0, |shard| u64::from(shard.next_doc_id()));
        if claimed != tip {
            return Err(Status::failed_precondition(format!(
                "WAL replay starts at document {claimed}, but this shard's next document is {tip}"
            )));
        }
        Ok(())
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

    pub(in crate::node::rpc) async fn handle_add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        crate::metrics::timed(Route::AddDocuments, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let addr = self.config.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable("no analysis backend configured for this shard (analysis_addr)")
            })?;
            let keys = StreamKeys::from_metadata(request.metadata())?;
            let replay = replay_first_id(request.metadata())?;
            if let Some(claimed) = replay {
                self.require_document_tip(claimed)?;
            }
            let mut inbound = request.into_inner();
            let mut source = IngestSource::Plain {
                stream: &mut inbound,
                keys,
                logged: replay.is_some(),
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

    pub(in crate::node::rpc) async fn handle_delete_documents(
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

    pub(in crate::node::rpc) async fn handle_commit_replacements(
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

    pub(in crate::node::rpc) async fn handle_ingest_mapped(
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
