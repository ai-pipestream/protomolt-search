//! Record admission and routing for [`WalWriter`].

use std::fs::File;
use std::io;

use super::sources;
use super::writer::WalWriter;
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors, LoggedBinding};
use crate::reshard::bucket_of;

fn invalid_input(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

/// A vector record's batch must decode, materialized rows included, before
/// it reaches the log.
fn validate_vectors(add: &LoggedAddVectors) -> io::Result<()> {
    let batch = add
        .batch
        .as_ref()
        .ok_or_else(|| invalid_input("missing vector batch"))?;
    crate::multi_res_wal::decode_batch(batch).map_err(invalid_input)?;
    Ok(())
}

/// A logged binding must carry a decodable analysis contract, vector
/// binding, and index contract, and a well-formed analysis digest.
fn validate_binding(binding: &LoggedBinding) -> io::Result<()> {
    crate::mapped_analysis::decode_contract(
        &binding.analysis_sha,
        &binding.analysis_contract,
        &binding.body_path,
    )
    .map_err(invalid_input)?;
    crate::mapped_vector::decode(&binding.vector_binding, &binding.plan_fingerprint)
        .map_err(invalid_input)?;
    crate::index_contract::validate_binding(
        &binding.index_contract,
        &binding.plan_fingerprint,
        &binding.vector_binding,
    )
    .map_err(invalid_input)?;
    if !binding.analysis_sha.is_empty()
        && !crate::mapped_analysis::valid_digest(&binding.analysis_sha)
    {
        return Err(invalid_input("invalid mapped analysis digest"));
    }
    Ok(())
}

/// The bucket a record's id hashes to; `None` for markers, which carry no
/// id and go to `markers.wal`.
fn hashed_bucket(op: &wal_record::Op, bucket_count: u32) -> Option<u32> {
    let id = match op {
        wal_record::Op::AddVectors(a) => a.first_id,
        wal_record::Op::AddDocuments(a) => a.first_id,
        wal_record::Op::DeleteDocument(d) => d.doc_id,
        wal_record::Op::Replacement(r) => r.old_doc_id,
        wal_record::Op::Flush(_) | wal_record::Op::Snapshot(_) | wal_record::Op::Bind(_) => {
            return None
        }
    };
    Some(bucket_of(id, bucket_count as usize) as u32)
}

impl WalWriter {
    /// Append `op` to `bucket` instead of the bucket its id hashes to.
    /// For a log the writer owns outright and replays itself without the
    /// hash-routing check (a re-placement split's spill cut by a column);
    /// a node's log is never written
    /// this way, because its replay routes by the hash.
    pub fn append_to_bucket(&mut self, bucket: u32, op: wal_record::Op) -> io::Result<()> {
        if bucket >= self.manifest.bucket_count {
            return Err(invalid_input(format!(
                "bucket {bucket} is outside the log's {} buckets",
                self.manifest.bucket_count
            )));
        }
        self.append_routed(op, Some(bucket))
    }

    /// Append one record. The node applies the mutation to the in-memory
    /// index FIRST and logs after, under one lock: both sides are
    /// volatile until Flush, and Flush fsyncs the log before it writes
    /// the index images, so every durable index state has its records
    /// durable too — while an apply that fails never reaches the log
    /// (a logged-but-unapplied record would poison the id sequence).
    /// Vector/document records route to their id's bucket file; markers
    /// go to `markers.wal`. Buffered only — no fsync.
    ///
    /// NOTE a `LoggedAddVectors` batch must not straddle buckets: the
    /// caller (the node) splits batches into per-vector records, so
    /// `first_id` routes the whole record.
    pub fn append(&mut self, op: wal_record::Op) -> io::Result<()> {
        self.append_routed(op, None)
    }

    /// Validate a document batch against the manifest and intern its
    /// original sources into `sources.wal`, rewriting them to addresses.
    fn admit_documents(&mut self, batch: &mut LoggedAddDocuments) -> io::Result<()> {
        if !batch.source_references.is_empty() {
            return Err(invalid_input(
                "WAL append requires resolved source references",
            ));
        }
        for document in &batch.documents {
            if document.derived_fingerprint != self.manifest.derived_fingerprint {
                return Err(invalid_input(
                    "WAL document derived fingerprint differs from its manifest declaration",
                ));
            }
            if let Some(identity) = &document.identity {
                if document.original_source.is_none() {
                    return Err(invalid_input("WAL identity requires original source"));
                }
                crate::source_archive::validate_identity(identity, document.source_chunk_ordinal)
                    .map_err(invalid_input)?;
            }
        }
        if batch
            .documents
            .iter()
            .any(|d| d.original_source.is_some() || d.source_chunk_ordinal.is_some())
        {
            if self.sources.is_none() {
                self.sources = Some(sources::Writer::open(&self.dir)?);
            }
            self.sources
                .as_mut()
                .expect("opened")
                .intern_documents(batch)?;
        }
        Ok(())
    }

    fn append_routed(&mut self, mut op: wal_record::Op, forced: Option<u32>) -> io::Result<()> {
        match &mut op {
            wal_record::Op::AddVectors(add) => validate_vectors(add)?,
            wal_record::Op::Bind(binding) => validate_binding(binding)?,
            wal_record::Op::AddDocuments(batch) => self.admit_documents(batch)?,
            _ => {}
        }
        let hashed = hashed_bucket(&op, self.manifest.bucket_count);
        // A forced bucket applies to record ops; markers keep their file.
        let bucket = match (forced, hashed) {
            (Some(forced), Some(_)) => Some(forced),
            (_, hashed) => hashed,
        };
        let clock = self.next_clock;
        let result = match bucket {
            Some(b) => {
                self.records_appended += 1;
                self.bucket(b)?.append(clock, &op)
            }
            None => self.markers.append(clock, &op),
        };
        if result.is_ok() {
            self.next_clock = self.next_clock.saturating_add(1);
            self.dirty = true;
        }
        result
    }

    /// Flush all buffers and fsync every open file. Called on Flush and
    /// on generation rotation.
    pub fn flush(&mut self) -> io::Result<()> {
        // Source bytes must be durable before any referencing row is fsynced.
        if let Some(sources) = self.sources.as_mut() {
            sources.flush()?;
        }
        for (bucket, writer) in &mut self.buckets {
            writer
                .flush()
                .map_err(|e| io::Error::new(e.kind(), format!("bucket {bucket}: {e}")))?;
        }
        self.markers.flush()?;
        File::open(&self.dir)?.sync_all()?;
        self.dirty = false;
        Ok(())
    }
}
