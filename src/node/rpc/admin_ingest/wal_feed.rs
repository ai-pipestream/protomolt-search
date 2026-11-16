//! WAL catch-up feed and replicated mapping-binding RPCs.

use crate::node::*;

/// Shard health as of one durable WAL prefix, reported on the feed's last frame.
struct PrefixHealth {
    num_vectors: u64,
    document_slots: u64,
    live_docs: u64,
    deleted_docs: u64,
    scoring_fingerprint: String,
}

/// The records after a requested clock plus the watermark they were read under.
struct WalPrefix {
    generation: u64,
    high_watermark: u64,
    records: Vec<crate::pb::wal::WalRecord>,
    health: PrefixHealth,
}

fn spawn_wal_feed(
    prefix: WalPrefix,
    slot_offset: u64,
) -> ReceiverStream<Result<ReadWalResponse, Status>> {
    let WalPrefix {
        generation,
        high_watermark,
        records,
        health,
    } = prefix;
    let (tx, rx) = mpsc::channel(64);
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
                num_vectors: health.num_vectors,
                document_slots: health.document_slots,
                live_docs: health.live_docs,
                deleted_docs: health.deleted_docs,
                scoring_fingerprint: health.scoring_fingerprint,
                slot_offset,
            }))
            .await;
    });
    ReceiverStream::new(rx)
}

impl NodeServiceImpl {
    /// Read the WAL records after `req.after_clock` and the shard health they
    /// correspond to, under one read lock.
    fn read_wal_prefix(&self, req: &ReadWalRequest) -> Result<WalPrefix, Status> {
        let guard = read_shard(&self.state);
        let wal = guard.wal.as_ref().ok_or_else(|| {
            Status::failed_precondition("this shard has no WAL; live catch-up is unavailable")
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
        let records = wal::read_clocked_records(wal.dir(), req.after_clock)
            .map_err(|error| Status::failed_precondition(format!("read WAL: {error}")))?;
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
        Ok(WalPrefix {
            generation: wal.generation(),
            high_watermark: wal.high_watermark(),
            records,
            health: PrefixHealth {
                num_vectors,
                document_slots,
                live_docs: physical_docs - deleted_docs,
                deleted_docs,
                scoring_fingerprint,
            },
        })
    }

    /// Flush until the records read end exactly at the advertised watermark.
    async fn durable_wal_prefix(&self, req: &ReadWalRequest) -> Result<WalPrefix, Status> {
        loop {
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
            let prefix = self.read_wal_prefix(req)?;
            // A writer can append after the dirty check and before the read
            // lock. Its buffered bytes then trail the in-memory watermark.
            // Never advertise that watermark: retry, which sees dirty=true
            // and makes the whole index/WAL prefix durable first.
            if prefix.high_watermark == req.after_clock
                || prefix
                    .records
                    .last()
                    .is_some_and(|record| record.clock == prefix.high_watermark)
            {
                return Ok(prefix);
            }
            tokio::task::yield_now().await;
        }
    }

    pub(in crate::node::rpc) async fn handle_read_wal(
        &self,
        request: Request<ReadWalRequest>,
    ) -> Result<Response<<Self as NodeService>::ReadWalStream>, Status> {
        crate::metrics::timed_stream(Route::ReadWal, request, |request| async move {
            let req = request.into_inner();
            let prefix = self.durable_wal_prefix(&req).await?;
            Ok(Response::new(spawn_wal_feed(
                prefix,
                self.config.slot_offset,
            )))
        })
        .await
    }

    pub(in crate::node::rpc) async fn handle_apply_wal_binding(
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
}
