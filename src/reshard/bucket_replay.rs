//! Reading WAL bucket files record by record into a [`Replay`].

use std::path::Path;

use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors};
use crate::wal::{self, RecordReader};

use super::bucket_of;
use super::replay::Replay;

/// How a bucket replay reads: the geometry it checks routing against,
/// the clock it stops at, and what it keeps.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReplayScope {
    /// The generation's bucket count, which routing is checked against.
    pub(super) bucket_count: usize,
    /// Floats per vector row.
    pub(super) dim: usize,
    /// Skip document records.
    pub(super) vectors_only: bool,
    /// Skip records past this generation clock; `u64::MAX` reads all.
    pub(super) upto_clock: u64,
    /// Refuse a record whose id does not hash to the file it sits in.
    /// A spill cut by a column files records by value, not by id hash.
    pub(super) check_routing: bool,
}

/// One bucket file being replayed: its path for messages and the bucket
/// it must route to.
#[derive(Clone, Copy)]
struct BucketFile<'a> {
    path: &'a Path,
    bucket: u32,
    scope: &'a ReplayScope,
}

impl BucketFile<'_> {
    fn routes_here(&self, id: u64) -> bool {
        bucket_of(id, self.scope.bucket_count) as u32 == self.bucket
    }

    fn misrouted(&self, id: u64) -> bool {
        self.scope.check_routing && !self.routes_here(id)
    }
}

/// Replay the bucket files in `buckets` of one generation into `out`
/// under `scope`. A bucket that never received a record has no file.
pub(super) fn replay_buckets_routed(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    scope: &ReplayScope,
    out: &mut Replay,
) -> Result<(), String> {
    for bucket in buckets {
        let path = wal::bucket_path(gen, bucket);
        if !path.exists() {
            continue;
        }
        let file = BucketFile {
            path: &path,
            bucket,
            scope,
        };
        replay_file(file, out)?;
    }
    Ok(())
}

fn replay_file(file: BucketFile<'_>, out: &mut Replay) -> Result<(), String> {
    let path = file.path;
    let mut reader =
        RecordReader::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    while let Some(record) = reader
        .next_record()
        .map_err(|e| format!("replay {}: {e}", path.display()))?
    {
        if !admits_clock(file, record.clock)? {
            continue;
        }
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => replay_vectors(file, add, out)?,
            Some(wal_record::Op::AddDocuments(add)) => replay_documents(file, add, out)?,
            Some(wal_record::Op::Bind(_)) => {
                return Err(format!(
                    "replay {}: a binding record in a bucket file — binds route to \
                     markers.wal, so this log is corrupt or foreign",
                    path.display()
                ));
            }
            Some(wal_record::Op::DeleteDocument(delete)) => {
                replay_delete(file, delete.doc_id, "delete id", out)?;
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                replay_delete(file, replacement.old_doc_id, "replacement old id", out)?;
            }
            Some(wal_record::Op::Flush(_)) | Some(wal_record::Op::Snapshot(_)) | None => {}
        }
    }
    Ok(())
}

/// Whether a record at `clock` is inside the scope's cutoff. A legacy
/// unclocked record (clock 0) cannot be placed against a bound.
fn admits_clock(file: BucketFile<'_>, clock: u64) -> Result<bool, String> {
    let upto_clock = file.scope.upto_clock;
    if upto_clock == u64::MAX {
        return Ok(true);
    }
    if clock == 0 {
        return Err(format!(
            "replay {}: a legacy unclocked record cannot be placed against the cutoff clock \
             {upto_clock}",
            file.path.display()
        ));
    }
    Ok(clock <= upto_clock)
}

/// Deletes and replacements always route by the id they hide, whether or
/// not the scope checks the routing of added rows.
fn replay_delete(
    file: BucketFile<'_>,
    id: u64,
    what: &str,
    out: &mut Replay,
) -> Result<(), String> {
    if !file.routes_here(id) {
        return Err(format!(
            "replay {}: {what} {id} routes to another bucket",
            file.path.display()
        ));
    }
    out.deleted.insert(id);
    Ok(())
}

fn replay_vectors(
    file: BucketFile<'_>,
    add: LoggedAddVectors,
    out: &mut Replay,
) -> Result<(), String> {
    let path = file.path;
    let dim = file.scope.dim;
    if file.misrouted(add.first_id) {
        return Err(format!(
            "replay {}: record id {} routes to bucket {}, not {} — corrupt log or changed \
             bucket geometry",
            path.display(),
            add.first_id,
            bucket_of(add.first_id, file.scope.bucket_count),
            file.bucket
        ));
    }
    let batch = add.batch.unwrap_or_default();
    if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
        return Err(format!(
            "replay {}: batch of {} floats is not a multiple of dim {dim}",
            path.display(),
            batch.vectors.len()
        ));
    }
    if !batch.materialized_rows.is_empty() && batch.dim as usize != dim {
        return Err(format!(
            "replay {}: materialized vector dimension differs from manifest",
            path.display()
        ));
    }
    crate::multi_res_wal::decode_batch(&batch)
        .map_err(|error| format!("replay {}: {error}", path.display()))?;
    let keys = &add.stable_routing_keys;
    for (i, vector) in batch.vectors.chunks_exact(dim).enumerate() {
        if !keys.is_empty() && keys.len() != batch.vectors.len() / dim {
            return Err(format!(
                "replay {}: vector stable-key count does not match rows",
                path.display()
            ));
        }
        let id = add.first_id + i as u64;
        if file.misrouted(id) {
            return Err(format!(
                "replay {}: vector batch straddles WAL buckets at id {id}",
                path.display()
            ));
        }
        if out.vectors.insert(id, vector.to_vec()).is_some() {
            return Err(format!(
                "replay {}: duplicate vector id {id}",
                path.display()
            ));
        }
        if let Some(retained) = batch.materialized_rows.get(i) {
            out.retained.insert(id, retained.clone());
        }
        record_stable_key(file, id, keys.get(i), out)?;
    }
    Ok(())
}

fn replay_documents(
    file: BucketFile<'_>,
    add: LoggedAddDocuments,
    out: &mut Replay,
) -> Result<(), String> {
    let path = file.path;
    if file.misrouted(add.first_id) {
        return Err(format!(
            "replay {}: document id {} routes to bucket {}, not {}",
            path.display(),
            add.first_id,
            bucket_of(add.first_id, file.scope.bucket_count),
            file.bucket
        ));
    }
    if file.scope.vectors_only {
        return Ok(());
    }
    let keys = &add.stable_routing_keys;
    if !keys.is_empty() && keys.len() != add.documents.len() {
        return Err(format!(
            "replay {}: document stable-key count does not match rows",
            path.display()
        ));
    }
    for (i, doc) in add.documents.into_iter().enumerate() {
        let id = add.first_id + i as u64;
        if file.misrouted(id) {
            return Err(format!(
                "replay {}: document batch straddles WAL buckets at id {id}",
                path.display()
            ));
        }
        if out.documents.insert(id, doc).is_some() {
            return Err(format!(
                "replay {}: duplicate document id {id}",
                path.display()
            ));
        }
        record_stable_key(file, id, keys.get(i), out)?;
    }
    Ok(())
}

/// A row's stable key, which its vector and document records must agree on.
fn record_stable_key(
    file: BucketFile<'_>,
    id: u64,
    key: Option<&Vec<u8>>,
    out: &mut Replay,
) -> Result<(), String> {
    let Some(key) = key else {
        return Ok(());
    };
    match out.stable_keys.insert(id, key.clone()) {
        Some(existing) if existing != *key => Err(format!(
            "replay {}: vector/document stable keys disagree for id {id}",
            file.path.display()
        )),
        _ => Ok(()),
    }
}
