//! The append records of a catch-up: admission against the replica's tips,
//! and runs of admitted rows held for one stream, keyed and unkeyed alike.
//!
//! A keyed record is cut by [`plan_keyed_streams`] into slices whose keys
//! fit one stream's metadata, and each slice joins the held keyed run when
//! it fits there. Every record is admitted, and every document checked
//! against the receiver's replication contract, before any of it is held,
//! so nothing a record refuses was sent for it.

use crate::pb::wal::{LoggedAddDocuments, LoggedAddVectors};
use crate::pb::{AddDocumentsRequest, AddVectorsRequest};

use super::batch::{Held, Piece, Run, MAX_STREAM_ROWS};
use super::sync::ReplicaWriter;
use super::vector_row;
use crate::replication_wire::{
    encode_replay_first_id, encode_stable_keys, plan_keyed_streams, require_receiver,
    STABLE_KEYS_METADATA, WAL_REPLAY_METADATA,
};

const DIFFERENT_VECTOR_IDS: &str = "replica assigned different vector ids";

/// A stream of `requests` carrying `keys` (one per row) when there are any.
fn keyed_stream<T>(
    requests: Vec<T>,
    keys: &[Vec<u8>],
) -> Result<tonic::Request<tokio_stream::Iter<std::vec::IntoIter<T>>>, String> {
    let mut request = tonic::Request::new(tokio_stream::iter(requests));
    if !keys.is_empty() {
        request.metadata_mut().insert_bin(
            STABLE_KEYS_METADATA,
            tonic::metadata::MetadataValue::from_bytes(&encode_stable_keys(keys)?),
        );
    }
    Ok(request)
}

/// Cut a record's `rows` rows into the pieces it is held as: the whole
/// record when unkeyed, one piece per planned keyed stream otherwise, when
/// `requests` holds one request per row.
fn pieces<T>(rows: u64, required: u32, keys: Vec<Vec<u8>>, requests: Vec<T>) -> Vec<Piece<T>> {
    if keys.is_empty() {
        return vec![Piece {
            rows,
            required,
            requests,
            keys,
        }];
    }
    let mut requests = requests.into_iter();
    plan_keyed_streams(&keys, MAX_STREAM_ROWS as usize)
        .into_iter()
        .map(|range| Piece {
            rows: range.len() as u64,
            required,
            requests: requests.by_ref().take(range.len()).collect(),
            keys: keys[range].to_vec(),
        })
        .collect()
}

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

    /// Refuse a keyed record before it is held when the replica cannot
    /// take keyed rows several to a stream.
    fn require_keyed(&self, keys: &[Vec<u8>], rows: u64, what: &str) -> Result<(), String> {
        if keys.is_empty() {
            return Ok(());
        }
        if keys.len() as u64 != rows {
            return Err(format!("WAL {what} stable-key count does not match rows"));
        }
        require_receiver(self.target.replication_version, "keyed append streams")
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
        self.require_keyed(&add.stable_routing_keys, rows, "vector")?;
        let tip = self.target.slot_offset + self.target.num_vectors;
        if !Self::admit(add.first_id, rows, tip, "vector")? {
            return Ok(());
        }
        let requests = if add.stable_routing_keys.is_empty() {
            vec![batch]
        } else {
            batch
                .vectors
                .chunks_exact(dim)
                .enumerate()
                .map(|(row, vector)| vector_row(&batch, row, vector))
                .collect()
        };
        let mut first_id = add.first_id;
        for piece in pieces(rows, 0, add.stable_routing_keys, requests) {
            let rows = piece.rows;
            self.hold_vectors(first_id, piece).await?;
            first_id += rows;
        }
        self.target.num_vectors += rows;
        Ok(())
    }

    /// Hold an admitted vector piece, joining the held vector run when it
    /// fits and sending whatever is held first when it does not.
    async fn hold_vectors(
        &mut self,
        first_id: u64,
        mut piece: Piece<AddVectorsRequest>,
    ) -> Result<(), String> {
        if let Held::Vectors(run) = &mut self.held {
            match run.extend(piece) {
                None => return Ok(()),
                Some(refused) => piece = refused,
            }
        }
        self.send_held().await?;
        self.held = Held::Vectors(Run::of(first_id, piece));
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
        if add
            .documents
            .iter()
            .any(|document| !document.derived_fingerprint.is_empty())
        {
            require_receiver(self.target.replication_version, "logged derived columns")?;
        }
        let rows = add.documents.len() as u64;
        if rows == 0 {
            return Err("WAL document record is empty".to_string());
        }
        self.require_keyed(&add.stable_routing_keys, rows, "document")?;
        let tip = self.target.slot_offset + self.target.document_slots;
        if !Self::admit(add.first_id, rows, tip, "document")? {
            return Ok(());
        }
        let mut first_id = add.first_id;
        for piece in pieces(rows, required, add.stable_routing_keys, add.documents) {
            let rows = piece.rows;
            self.hold_documents(first_id, piece).await?;
            first_id += rows;
        }
        self.target.document_slots += rows;
        Ok(())
    }

    /// Hold an admitted document piece, joining the held document run when
    /// it fits and sending whatever is held first when it does not.
    async fn hold_documents(
        &mut self,
        first_id: u64,
        mut piece: Piece<AddDocumentsRequest>,
    ) -> Result<(), String> {
        if let Held::Documents(run) = &mut self.held {
            match run.extend(piece) {
                None => return Ok(()),
                Some(refused) => piece = refused,
            }
        }
        self.send_held().await?;
        self.held = Held::Documents(Run::of(first_id, piece));
        Ok(())
    }

    /// Send the held run as one stream and hold the replica to the ids
    /// and row count the run was admitted under.
    pub(super) async fn send_held(&mut self) -> Result<(), String> {
        match std::mem::replace(&mut self.held, Held::Nothing) {
            Held::Nothing => Ok(()),
            Held::Documents(run) => {
                let mut request = keyed_stream(run.requests, &run.keys)?;
                request.metadata_mut().insert_bin(
                    WAL_REPLAY_METADATA,
                    tonic::metadata::MetadataValue::from_bytes(&encode_replay_first_id(
                        run.first_id,
                    )),
                );
                self.add_documents(request, run.required, run.first_id, run.rows)
                    .await
            }
            Held::Vectors(run) => {
                let request = keyed_stream(run.requests, &run.keys)?;
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
