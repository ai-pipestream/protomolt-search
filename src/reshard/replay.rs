//! WAL replay into per-bucket record sets, and the generation and backend checks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::pb::wal::wal_record;
use crate::pb::AddDocumentsRequest;
use crate::postings::AnalyzedField;
use crate::wal::{self, RecordReader, WalManifest};

pub(super) use super::bucket_replay::{replay_buckets_routed, ReplayScope};

/// Vectors and documents replayed out of bucket files, keyed by their
/// server-assigned global ids (BTreeMap iteration IS id order).
#[derive(Default)]
pub(super) struct Replay {
    /// global id -> one vector (`dim` floats).
    pub(super) vectors: BTreeMap<u64, Vec<f32>>,
    /// Validated materialized payload by source id; never re-project active rows.
    pub(super) retained: BTreeMap<u64, Vec<u8>>,
    /// global id -> the document as ingested.
    pub(super) documents: BTreeMap<u64, AddDocumentsRequest>,
    /// Rows hidden by delete or replacement records. Build drops them,
    /// producing a dense all-live child generation.
    pub(super) deleted: std::collections::BTreeSet<u64>,
    /// Stable product identities learned from routed WAL envelopes.
    pub(super) stable_keys: BTreeMap<u64, Vec<u8>>,
    /// global id -> the document's analyzed fields, table order, when
    /// the replay carries a transplant sidecar
    /// and the child build takes the
    /// fields from here instead of the analyzer.
    pub(super) analyzed: BTreeMap<u64, Vec<AnalyzedField>>,
}

impl Replay {
    pub(super) fn compact(&mut self) {
        for id in &self.deleted {
            self.vectors.remove(id);
            self.retained.remove(id);
            self.documents.remove(id);
            self.stable_keys.remove(id);
            self.analyzed.remove(id);
        }
        self.deleted.clear();
    }
}

/// Resolve a `--log` argument: a generation directory is used directly; a
/// WAL directory contributes its newest generation (a snapshot install
/// supersedes earlier generations, so the latest one is the live log).
pub fn resolve_gen(path: &Path) -> Result<PathBuf, String> {
    if path.is_dir() {
        if wal::manifest_path(path).exists() {
            return Ok(path.to_path_buf());
        }
        wal::latest_gen(path)
            .map_err(|e| format!("list {}: {e}", path.display()))?
            .map(|(_, p)| p)
            .ok_or_else(|| format!("no WAL generation directories in {}", path.display()))
    } else {
        Err(format!(
            "{}: not a WAL directory or generation directory",
            path.display()
        ))
    }
}

pub(super) fn read_gen_manifest(gen: &Path) -> Result<WalManifest, String> {
    wal::read_manifest(gen).map_err(|e| format!("read manifest of {}: {e}", gen.display()))
}

/// Replay the bucket files in `buckets` of one generation into `out`.
/// Every record must route to the file it was read from (the node routes
/// by the same [`bucket_of`](super::bucket_of)); a violation means the log is corrupt or
/// the bucket geometry changed mid-generation.
pub(super) fn replay_buckets(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    bucket_count: usize,
    dim: usize,
    vectors_only: bool,
    out: &mut Replay,
) -> Result<(), String> {
    replay_buckets_through(gen, buckets, bucket_count, dim, vectors_only, u64::MAX, out)
}

/// [`replay_buckets`] bounded by the generation clock: records past `upto_clock`
/// are skipped, which is how an online compaction builds its dense image from a
/// prefix of a log that keeps growing under it. A legacy unclocked record (clock 0)
/// is refused under a bound, because it cannot be placed against the cutoff.
pub(super) fn replay_buckets_through(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    bucket_count: usize,
    dim: usize,
    vectors_only: bool,
    upto_clock: u64,
    out: &mut Replay,
) -> Result<(), String> {
    let scope = ReplayScope {
        bucket_count,
        dim,
        vectors_only,
        upto_clock,
        check_routing: true,
    };
    replay_buckets_routed(gen, buckets, &scope, out)
}

/// Require usable, locked provider state in the manifest (see the module
/// docs for why incomplete provider state cannot be resharded).
pub(super) fn require_backend_config(manifest: &WalManifest, what: &Path) -> Result<(), String> {
    let dim = manifest.dim as usize;
    let config = manifest.backend_config();
    let configured = config.as_ref().is_ok_and(|config| {
        crate::vector::legacy_calibration_config(config)
            .ok()
            .flatten()
            .is_none_or(|legacy| legacy.shift.len() == dim && legacy.scale.len() == dim)
    });
    if dim == 0 || !configured {
        return Err(format!(
            "{}: WAL manifest carries no usable locked vector backend configuration (dim {}); \
             resharding requires provider state that can reproduce byte-comparable shard scores",
            what.display(),
            manifest.dim
        ));
    }
    Ok(())
}

/// Require full history: a generation whose manifest records preexisting
/// state (an installed image, or logging enabled on an already-populated
/// shard) does not contain everything the shard holds, and a log-only
/// replay would silently drop that state. Hard error, like missing provider
/// configuration.
pub(super) fn require_complete_history(manifest: &WalManifest, what: &Path) -> Result<(), String> {
    if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
        return Err(format!(
            "{}: generation {} began with {} preexisting vector(s) and {} preexisting \
             document(s) that are NOT in this log (an installed snapshot image, or logging \
             enabled after data existed); replaying it would drop them. Reshard from a \
             full-history generation, or rebuild the shard from source",
            what.display(),
            manifest.generation,
            manifest.preexisting_vectors,
            manifest.preexisting_documents
        ));
    }
    Ok(())
}

/// The mapped-plan binding carried by the input generations' markers
/// files: every Bind record
/// across every input must agree, because the children are ONE corpus
/// under ONE plan — contradictory bindings mean the inputs were mapped
/// under different plans and their columns must not merge. `None` when
/// no input was ever bound (hand-built columns bind nothing). Every split,
/// merge and compaction reads its inputs' binding here before it replays,
/// so the inputs are first held to one derived-column declaration.
pub(super) fn read_gens_binding(
    gens: &[PathBuf],
) -> Result<Option<crate::postings::StoredBinding>, String> {
    super::merge_inputs::require_one_declaration(gens)?;
    let mut bound: Option<(crate::postings::StoredBinding, PathBuf)> = None;
    for gen in gens {
        let path = wal::markers_path(gen);
        if !path.exists() {
            continue;
        }
        let mut reader =
            RecordReader::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        while let Some(record) = reader
            .next_record()
            .map_err(|e| format!("replay {}: {e}", path.display()))?
        {
            let Some(wal_record::Op::Bind(bind)) = record.op else {
                continue;
            };
            let binding = crate::postings::StoredBinding {
                plan_fingerprint: bind.plan_fingerprint,
                body_path: bind.body_path,
                materialize_sha: bind.materialize_sha,
                analysis_sha: bind.analysis_sha,
                analysis_contract: bind.analysis_contract,
                vector_binding: bind.vector_binding,
                index_contract: bind.index_contract,
            };
            crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )?;
            crate::mapped_vector::decode(&binding.vector_binding, &binding.plan_fingerprint)
                .map_err(|e| e.to_string())?;
            crate::index_contract::validate_binding(
                &binding.index_contract,
                &binding.plan_fingerprint,
                &binding.vector_binding,
            )
            .map_err(|e| e.to_string())?;
            match &bound {
                Some((first, first_gen)) if *first != binding => {
                    return Err(format!(
                        "{}: bound to plan {} but {} is bound to plan {}; the inputs were \
                         mapped under different plans and their columns must not combine",
                        path.display(),
                        binding.plan_fingerprint,
                        first_gen.display(),
                        first.plan_fingerprint
                    ));
                }
                Some(_) => {}
                None => bound = Some((binding, gen.clone())),
            }
        }
    }
    Ok(bound.map(|(binding, _)| binding))
}

/// The mapped-plan binding one generation's markers carry, if any
/// (`read_gens_binding` over one input).
pub fn read_generation_binding(
    gen: &Path,
) -> Result<Option<crate::postings::StoredBinding>, String> {
    read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))
}

/// Provider scoring identity for the merge check (slot offset and generation
/// legitimately differ between inputs; the shape and provider state must not).
pub(super) fn same_backend_config(a: &WalManifest, b: &WalManifest) -> bool {
    // One dataset, one provider state: generations of two collections
    // never merge, whatever their vectors look like.
    a.collection == b.collection
        && a.dim == b.dim
        && a.backend_config().ok() == b.backend_config().ok()
}
