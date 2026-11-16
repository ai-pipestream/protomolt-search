//! The append records of a catch-up: admission against the replica's tips,
//! unkeyed runs held for one stream, and keyed rows sent one per stream.

use crate::pb::wal::{LoggedAddDocuments, LoggedAddVectors};
use crate::pb::{AddDocumentsRequest, AddVectorsRequest};

use super::batch::{Held, Run};
use super::sync::ReplicaWriter;
use super::{keyed_request, require_fresh_ingest_replay, vector_row};

const DIFFERENT_VECTOR_IDS: &str = "replica assigned different vector ids";

impl ReplicaWriter {
    /// Where a record of `rows` rows from `first_id` stands against the
    /// replica's tip: `Ok(true)` to append, `Ok(false)` when it is already
    /// present, an error for a partial batch or a gap.
    fn admit(first_id: u64, rows: u64, tip: u64, what: &str) -> Result<bool, String> {
        if first_id < tip {
            if first_id + rows > tip {
                return Err(format!("replica contains a partial WAL {what} batch"));
            }
            return Ok(false);
        }
        if first_id == tip {
            return Ok(true);
        }
        Err(format!(
            "replica {what} gap: next id is {tip}, WAL starts at {first_id}"
        ))
    }

    pub(super) async fn apply_vectors(&mut self, add: LoggedAddVectors) -> Result<(), String> {
        let batch = add
            .batch
            .ok_or_else(|| "WAL vector record has no batch".to_string())?;
        crate::multi_res_wal::require_supported(self.target.multi_res_vector_version, &batch)?;
        crate::multi_res_wal::decode_batch(&batch)?;
        let dim = usize::try_from(batch.dim).unwrap_or(0);
        if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
            return Err("WAL vector record has invalid dimensions".to_string());
        }
        let rows = (batch.vectors.len() / dim) as u64;
        if rows == 0 {
            return Err("WAL vector record is empty".to_string());
        }
        let keys = add.stable_routing_keys;
        if !keys.is_empty() && keys.len() != rows as usize {
            return Err("WAL vector stable-key count does not match rows".to_string());
        }
        let tip = self.target.slot_offset + self.target.num_vectors;
        if !Self::admit(add.first_id, rows, tip, "vector")? {
            return Ok(());
        }
        if keys.is_empty() {
            self.hold_vectors(add.first_id, rows, batch).await?;
        } else {
            self.send_held().await?;
            for (offset, (vector, key)) in batch.vectors.chunks_exact(dim).zip(&keys).enumerate() {
                let row = vector_row(&batch, offset, vector);
                let response = self.add_vectors(keyed_request(row, key)).await?;
                if response.first_id != add.first_id + offset as u64 || response.added != 1 {
                    return Err(DIFFERENT_VECTOR_IDS.to_string());
                }
            }
        }
        self.target.num_vectors += rows;
        Ok(())
    }

    /// Hold an admitted unkeyed vector record, joining the held vector run
    /// when it fits and sending whatever is held first when it does not.
    async fn hold_vectors(
        &mut self,
        first_id: u64,
        rows: u64,
        batch: AddVectorsRequest,
    ) -> Result<(), String> {
        let mut requests = vec![batch];
        if let Held::Vectors(run) = &mut self.held {
            match run.extend(rows, 0, requests) {
                None => return Ok(()),
                Some(refused) => requests = refused,
            }
        }
        self.send_held().await?;
        self.held = Held::Vectors(Run::of(first_id, rows, 0, requests));
        Ok(())
    }

    async fn add_vectors(
        &mut self,
        request: tonic::Request<
            impl tokio_stream::Stream<Item = AddVectorsRequest> + Send + 'static,
        >,
    ) -> Result<crate::pb::AddVectorsResponse, String> {
        Ok(self
            .client
            .add_vectors(request)
            .await
            .map_err(|error| format!("replicate vectors: {error}"))?
            .into_inner())
    }

    pub(super) async fn apply_documents(&mut self, add: LoggedAddDocuments) -> Result<(), String> {
        let required = add
            .documents
            .iter()
            .map(crate::document_contract::required_version)
            .max()
            .unwrap_or(0);
        crate::document_contract::require_supported(
            self.target.document_contract_version,
            required,
        )?;
        for document in &add.documents {
            require_fresh_ingest_replay(document)?;
        }
        let rows = add.documents.len() as u64;
        if rows == 0 {
            return Err("WAL document record is empty".to_string());
        }
        let keys = add.stable_routing_keys;
        if !keys.is_empty() && keys.len() != rows as usize {
            return Err("WAL document stable-key count does not match rows".to_string());
        }
        let tip = self.target.slot_offset + self.target.document_slots;
        if !Self::admit(add.first_id, rows, tip, "document")? {
            return Ok(());
        }
        if keys.is_empty() {
            self.hold_documents(add.first_id, required, add.documents)
                .await?;
        } else {
            self.send_held().await?;
            for (offset, (document, key)) in add.documents.into_iter().zip(&keys).enumerate() {
                let required = crate::document_contract::required_version(&document);
                let first_id = add.first_id + offset as u64;
                self.add_documents(keyed_request(document, key), required, first_id, 1)
                    .await?;
            }
        }
        self.target.document_slots += rows;
        Ok(())
    }

    /// Hold an admitted unkeyed document record, joining the held document
    /// run when it fits and sending whatever is held first when it does
    /// not. Every document held has passed [`require_fresh_ingest_replay`].
    async fn hold_documents(
        &mut self,
        first_id: u64,
        required: u32,
        documents: Vec<AddDocumentsRequest>,
    ) -> Result<(), String> {
        let rows = documents.len() as u64;
        let mut requests = documents;
        if let Held::Documents(run) = &mut self.held {
            match run.extend(rows, required, requests) {
                None => return Ok(()),
                Some(refused) => requests = refused,
            }
        }
        self.send_held().await?;
        self.held = Held::Documents(Run::of(first_id, rows, required, requests));
        Ok(())
    }

    /// Send the held run as one stream and hold the replica to the ids
    /// and row count the run was admitted under.
    pub(super) async fn send_held(&mut self) -> Result<(), String> {
        match std::mem::replace(&mut self.held, Held::Nothing) {
            Held::Nothing => Ok(()),
            Held::Documents(run) => {
                let request = tonic::Request::new(tokio_stream::iter(run.requests));
                self.add_documents(request, run.required, run.first_id, run.rows)
                    .await
            }
            Held::Vectors(run) => {
                let request = tonic::Request::new(tokio_stream::iter(run.requests));
                let response = self.add_vectors(request).await?;
                if response.first_id != run.first_id || response.added != run.rows {
                    return Err(DIFFERENT_VECTOR_IDS.to_string());
                }
                Ok(())
            }
        }
    }

    /// Append documents and check the replica assigned `(first_id, rows)`
    /// under a contract version of at least `required`.
    async fn add_documents(
        &mut self,
        request: tonic::Request<
            impl tokio_stream::Stream<Item = AddDocumentsRequest> + Send + 'static,
        >,
        required: u32,
        first_id: u64,
        rows: u64,
    ) -> Result<(), String> {
        let response = self
            .client
            .add_documents(request)
            .await
            .map_err(|error| format!("replicate documents: {error}"))?
            .into_inner();
        crate::document_contract::require_supported(response.document_contract_version, required)?;
        if response.first_id != first_id || response.added != rows {
            return Err("replica assigned different document ids".to_string());
        }
        Ok(())
    }
}
