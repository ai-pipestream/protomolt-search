//! One replica brought to its primary's durable WAL high watermark.

use prost::Message;
use tonic::transport::Channel;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::wal::{wal_record, LoggedBinding, WalRecord};
use crate::pb::{
    ApplyWalBindingRequest, CommitReplacementsRequest, DeleteDocumentsRequest, FlushRequest,
    HealthRequest, HealthResponse, ReadWalRequest, ReadWalResponse,
};

use super::batch::Held;
use super::{client, ReplicaCursor};

/// The primary's counts at the end of the exported prefix.
struct PrefixHealth {
    vectors: u64,
    documents: u64,
    live: u64,
    deleted: u64,
    scoring: String,
    slot_offset: u64,
}

impl PrefixHealth {
    fn of(frame: ReadWalResponse) -> Self {
        Self {
            vectors: frame.num_vectors,
            documents: frame.document_slots,
            live: frame.live_docs,
            deleted: frame.deleted_docs,
            scoring: frame.scoring_fingerprint,
            slot_offset: frame.slot_offset,
        }
    }
}

/// The replica a catch-up writes to, with its append tips as they move.
/// The tips count every admitted row, held or sent.
pub(super) struct ReplicaWriter {
    pub(super) client: NodeServiceClient<Channel>,
    pub(super) target: HealthResponse,
    /// An earlier attempt may have applied a record but failed before
    /// flushing or acknowledging it. In-memory tips and already_bound do
    /// not prove durability, so retries must cross the flush boundary too.
    requires_flush: bool,
    /// Admitted append records waiting to share one stream.
    pub(super) held: Held,
}

/// Bring one replica to the primary's current durable WAL high watermark.
pub async fn sync_once(cursor: &ReplicaCursor) -> Result<ReplicaCursor, String> {
    let mut primary = client(&cursor.primary)?;
    let mut replica = client(&cursor.replica)?;
    let source = primary
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("primary health {}: {error}", cursor.primary))?
        .into_inner();
    if !source.wal_clocked {
        return Err(format!(
            "primary {} has no fully clocked WAL generation",
            cursor.primary
        ));
    }
    let generation = source.wal_generation;
    if cursor.clock != 0 && cursor.wal_generation != generation {
        return Err(format!(
            "primary WAL rotated from generation {} to {generation}; install the new base snapshot before resuming replica catch-up",
            cursor.wal_generation
        ));
    }
    let target = replica
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("replica health {}: {error}", cursor.replica))?
        .into_inner();
    if source.slot_offset != target.slot_offset {
        return Err(format!(
            "replica slot offset {} does not match primary {}",
            target.slot_offset, source.slot_offset
        ));
    }
    let after_clock = if cursor.wal_generation == generation {
        cursor.clock
    } else {
        0
    };
    let stream = primary
        .read_wal(ReadWalRequest {
            generation,
            after_clock,
        })
        .await
        .map_err(|error| format!("read primary WAL: {error}"))?
        .into_inner();
    let mut writer = ReplicaWriter {
        client: replica,
        target,
        requires_flush: false,
        held: Held::Nothing,
    };
    let (high_watermark, prefix) = writer
        .apply_stream(stream, generation, cursor.clock)
        .await?;
    if prefix.slot_offset != source.slot_offset {
        return Err("WAL prefix slot offset differs from primary health".to_string());
    }
    writer.flush_and_verify(&prefix).await?;
    Ok(ReplicaCursor {
        primary: cursor.primary.clone(),
        replica: cursor.replica.clone(),
        wal_generation: generation,
        clock: high_watermark,
    })
}

impl ReplicaWriter {
    /// Apply every record of the stream; returns the high watermark and
    /// the prefix health the completion frame carried.
    async fn apply_stream(
        &mut self,
        mut stream: tonic::Streaming<ReadWalResponse>,
        generation: u64,
        mut high_watermark: u64,
    ) -> Result<(u64, PrefixHealth), String> {
        let mut prefix = None;
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|error| format!("stream primary WAL: {error}"))?
        {
            if frame.generation != generation {
                return Err("primary changed WAL generation inside one catch-up stream".to_string());
            }
            high_watermark = frame.high_watermark;
            if frame.completed {
                if !frame.record.is_empty() {
                    return Err("WAL completion frame carries record bytes".to_string());
                }
                prefix = Some(PrefixHealth::of(frame));
                continue;
            }
            if prefix.is_some() {
                return Err("WAL record arrived after completion".to_string());
            }
            let record = WalRecord::decode(frame.record.as_slice())
                .map_err(|error| format!("decode WAL record: {error}"))?;
            self.apply(record).await?;
        }
        let Some(prefix) = prefix else {
            return Err("primary WAL stream ended without completion".to_string());
        };
        self.send_held().await?;
        Ok((high_watermark, prefix))
    }

    async fn apply(&mut self, record: WalRecord) -> Result<(), String> {
        self.requires_flush |= matches!(
            record.op.as_ref(),
            Some(
                wal_record::Op::AddVectors(_)
                    | wal_record::Op::AddDocuments(_)
                    | wal_record::Op::DeleteDocument(_)
                    | wal_record::Op::Replacement(_)
                    | wal_record::Op::Bind(_)
            )
        );
        // A held append run must land before any record that is not an
        // append, so the replica applies the source's order.
        if matches!(
            record.op.as_ref(),
            Some(
                wal_record::Op::DeleteDocument(_)
                    | wal_record::Op::Replacement(_)
                    | wal_record::Op::Bind(_)
            )
        ) {
            self.send_held().await?;
        }
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => self.apply_vectors(add).await,
            Some(wal_record::Op::AddDocuments(add)) => self.apply_documents(add).await,
            Some(wal_record::Op::DeleteDocument(delete)) => {
                let request = DeleteDocumentsRequest {
                    expected_wal_generation: None,
                    doc_ids: vec![delete.doc_id],
                };
                self.client
                    .delete_documents(request)
                    .await
                    .map_err(|error| format!("replicate delete: {error}"))?;
                Ok(())
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                let request = CommitReplacementsRequest {
                    expected_wal_generation: None,
                    replacements: vec![crate::pb::Replacement {
                        old_doc_id: replacement.old_doc_id,
                        new_doc_id: replacement.new_doc_id,
                    }],
                };
                self.client
                    .commit_replacements(request)
                    .await
                    .map_err(|error| format!("replicate replacement: {error}"))?;
                Ok(())
            }
            Some(wal_record::Op::Snapshot(snapshot)) => Err(format!(
                "source WAL contains snapshot marker from generation {}; install its base image before catch-up",
                snapshot.source_generation
            )),
            Some(wal_record::Op::Bind(binding)) => self.apply_binding(binding).await,
            Some(wal_record::Op::Flush(_)) | None => Ok(()),
        }
    }

    async fn apply_binding(&mut self, binding: LoggedBinding) -> Result<(), String> {
        let analysis_sha = binding.analysis_sha.clone();
        let vector_binding = binding.vector_binding.clone();
        let index_contract = binding.index_contract.clone();
        let response = self
            .client
            .apply_wal_binding(ApplyWalBindingRequest {
                collection: String::new(),
                plan_fingerprint: binding.plan_fingerprint,
                body_path: binding.body_path,
                materialize_sha: binding.materialize_sha,
                analysis_sha: binding.analysis_sha,
                analysis_contract: binding.analysis_contract,
                vector_binding: binding.vector_binding,
                index_contract: binding.index_contract,
            })
            .await
            .map_err(|error| format!("replicate mapped binding: {error}"))?
            .into_inner();
        if response.analysis_sha != analysis_sha {
            return Err(
                "replica did not acknowledge the mapped analysis contract; upgrade the receiver"
                    .into(),
            );
        }
        if response.vector_binding != vector_binding {
            return Err(
                "replica did not acknowledge the mapped vector binding; upgrade the receiver"
                    .into(),
            );
        }
        if response.index_contract != index_contract {
            return Err(
                "replica did not acknowledge the explicit index contract; upgrade the receiver"
                    .into(),
            );
        }
        Ok(())
    }

    /// Persist what was applied, then hold the replica's counts and
    /// scoring fingerprint to the primary's prefix.
    async fn flush_and_verify(&mut self, prefix: &PrefixHealth) -> Result<(), String> {
        if self.requires_flush {
            let flushed = self
                .client
                .flush(FlushRequest {})
                .await
                .map_err(|error| format!("flush replica: {error}"))?
                .into_inner();
            if !flushed.written {
                return Err("replica did not persist the WAL prefix (Flush.written=false)".into());
            }
        }
        let verified = self
            .client
            .health(HealthRequest {})
            .await
            .map_err(|error| format!("verify replica: {error}"))?
            .into_inner();
        for (name, expected, actual) in [
            ("vectors", prefix.vectors, verified.num_vectors),
            ("documents", prefix.documents, verified.document_slots),
            ("live rows", prefix.live, verified.live_docs),
            ("deleted rows", prefix.deleted, verified.deleted_docs),
        ] {
            if expected != actual {
                return Err(format!(
                    "replica verification failed for {name}: primary {expected}, replica {actual}"
                ));
            }
        }
        if prefix.scoring != verified.scoring_fingerprint {
            return Err("replica scoring fingerprint differs from primary".to_string());
        }
        Ok(())
    }
}
