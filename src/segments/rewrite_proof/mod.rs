//! Compare logical rows without a whole-field transpose or an in-memory key set.
use super::*;
use crate::pb::storage::SourceRewriteCertificate;
use crate::reshard::{reconstruct_document, segment_tables, SourceTables};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use turbo_sha256::Sha256;

mod rows;
mod schema;
#[cfg(test)]
mod tests;

use rows::fill;
use schema::{backend, schema, schema_hash, Schema};

const BEFORE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("before");
const AFTER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("after");
/// The certificate this module writes. Format 1 covered stored content and
/// exact FP32 rows; format 2 adds the provider's own encoded rows. Format 3
/// also covers lattice arc lengths and lossless SPLADE impacts/scales.
pub const CERTIFICATE_FORMAT: u32 = 3;
/// Digest bytes held per batch row.
pub const BATCH_ROW_BYTES: usize = 32;
/// The largest batch: 32 MiB of digests.
pub const MAX_BATCH_ROWS: usize = 1_048_576;
/// The batch a caller gets by asking for none (`proof_batch_rows == 0`): the
/// largest, because a smaller batch buys nothing but repeated vocabulary
/// passes (`proof_batch_rows_for_budget`).
pub const DEFAULT_PROOF_BATCH_ROWS: usize = MAX_BATCH_ROWS;

/// The batch size a digest budget of `bytes` affords, capped at
/// `MAX_BATCH_ROWS`. Fewer than 32 bytes cannot hold one row and refuse.
/// The proof walks every term of every field once per
/// batch of each segment, so the number of vocabulary passes is
/// `sum over segments of ceil(rows / batch)` per field: the batch bounds the
/// digest memory and nothing else, and the result is identical at any size.
pub fn proof_batch_rows_for_budget(bytes: u64) -> Result<usize, String> {
    if bytes < BATCH_ROW_BYTES as u64 {
        return Err(err("digest budget cannot hold one 32-byte proof row"));
    }
    Ok(usize::try_from(bytes / BATCH_ROW_BYTES as u64)
        .unwrap_or(MAX_BATCH_ROWS)
        .min(MAX_BATCH_ROWS))
}

#[cfg(test)]
type ScratchObserver = Box<dyn FnOnce(&Path)>;
#[cfg(test)]
thread_local! {
    /// Vocabulary passes the current thread's proofs have made: one per
    /// (segment, field, batch) posting walk.
    pub(crate) static VOCABULARY_PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Observes the private scratch after its directory and database exist.
    pub(crate) static AFTER_SCRATCH_CREATED: std::cell::RefCell<Option<ScratchObserver>> =
        const { std::cell::RefCell::new(None) };
}

fn err(error: impl std::fmt::Display) -> String {
    format!("source rewrite proof: {error}")
}
fn part(hash: &mut Sha256, bytes: &[u8]) {
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}
fn extend(previous: &[u8; 32], bytes: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(previous);
    part(&mut hash, bytes);
    hash
}
fn names(hash: &mut Sha256, values: &[String]) {
    hash.update(&(values.len() as u64).to_le_bytes());
    for value in values {
        part(hash, value.as_bytes());
    }
}

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Create the proof's scratch directory and its row database. Private from
/// the first syscall: the directory is created 0700 and the database file
/// 0600, so a permissive umask never exposes document identities through
/// the scratch.
fn create_scratch(scratch: &Path) -> Result<(Scratch, Database), String> {
    let mut directory = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }
    directory.create(scratch).map_err(err)?;
    let scratch = Scratch(scratch.to_path_buf());
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(scratch.0.join("rows.redb")).map_err(err)?;
    let mut builder = Database::builder();
    builder.set_cache_size(8 * 1024 * 1024);
    let backend = redb::backends::FileBackend::new(file).map_err(err)?;
    let database = builder.create_with_backend(backend).map_err(err)?;
    #[cfg(test)]
    if let Some(observe) = AFTER_SCRATCH_CREATED.with(|hook| hook.borrow_mut().take()) {
        observe(&scratch.0);
    }
    Ok((scratch, database))
}

/// Walk both row tables in identity order, refusing the first difference;
/// the digest of every identity and row digest they share.
fn compare_rows(database: &Database) -> Result<Vec<u8>, String> {
    let tx = database.begin_read().map_err(err)?;
    let before = tx.open_table(BEFORE).map_err(err)?;
    let after = tx.open_table(AFTER).map_err(err)?;
    let mut hash = Sha256::new();
    part(&mut hash, b"protomolt.source-rewrite.rows.v3");
    for (left, right) in before.iter().map_err(err)?.zip(after.iter().map_err(err)?) {
        let (lk, lv) = left.map_err(err)?;
        let (rk, rv) = right.map_err(err)?;
        if lk.value() != rk.value() || lv.value() != rv.value() {
            return Err(err(
                "document identity, stored query content, exact vector or encoded dense \
                 row differs",
            ));
        }
        part(&mut hash, lk.value());
        part(&mut hash, lv.value());
    }
    Ok(hash.finish().0.to_vec())
}

/// The digest of the schema and vector backend state both views share.
fn schema_digest(
    set: &OpenedSegmentSet,
    schema: Option<&Schema>,
    backend: Option<(usize, crate::vector::VectorBackendConfig)>,
) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(&schema_hash(set, schema));
    if let Some((dim, config)) = backend {
        digest.update(&(dim as u64).to_le_bytes());
        part(&mut digest, config.backend_kind.as_bytes());
        part(&mut digest, config.config_format.as_bytes());
        part(&mut digest, &config.payload);
    }
    digest.finish().0.to_vec()
}

impl OpenedSegmentSet {
    /// Compare every live document/chunk, its stored query content, its exact
    /// FP32 row and the vector provider's own encoding of it. Physical slots
    /// and segment cuts may differ. Duplicate identities refuse, even if their
    /// values match. The result is not permission to publish either view.
    ///
    /// `scratch` must not exist; it is created private (0700, its database
    /// 0600) and only this method's directory is removed. Digests use
    /// [`BATCH_ROW_BYTES`] per batch row (`1..=MAX_BATCH_ROWS`), plus one
    /// reconstructed row/posting, bounded provider transcript pieces and an
    /// 8 MiB disk-table cache. Mapped vector images stay unmaterialized. No whole source
    /// corpus, vocabulary transpose or identity set is materialized. The
    /// vocabulary of every field is walked once per batch of each segment.
    pub fn verify_source_rewrite(
        &self,
        after: &OpenedSegmentSet,
        scratch: &Path,
        batch_rows: usize,
    ) -> Result<SourceRewriteCertificate, String> {
        if !(1..=MAX_BATCH_ROWS).contains(&batch_rows) {
            return Err(err("batch_rows must be 1..=1048576"));
        }
        let owner = self
            .manifest()
            .source_owner
            .as_ref()
            .ok_or_else(|| err("before catalog has no source owner"))?;
        if after.manifest().source_owner.as_ref() != Some(owner)
            || self.binding() != after.binding()
        {
            return Err(err("owner or mapped binding changed"));
        }
        let before_schema = schema(self)?;
        let after_schema = schema(after)?;
        if before_schema != after_schema {
            return Err(err(
                "rewrite changes schema or drops empty-column declarations",
            ));
        }
        let before_backend = backend(self)?;
        let after_backend = backend(after)?;
        if before_backend != after_backend {
            return Err(err("rewrite changes or drops vector backend state"));
        }
        // The guard removes the scratch after the database closes.
        let (_scratch, database) = create_scratch(scratch)?;
        let rows = fill(&database, BEFORE, self, batch_rows)?;
        if rows != fill(&database, AFTER, after, batch_rows)? {
            return Err(err("live row count differs"));
        }
        let identity_content_sha256 = compare_rows(&database)?;
        Ok(SourceRewriteCertificate {
            format_version: CERTIFICATE_FORMAT,
            owner: Some(owner.decode()?),
            live_rows: rows,
            schema_sha256: schema_digest(self, before_schema.as_ref(), before_backend),
            identity_content_sha256,
        })
    }
}
