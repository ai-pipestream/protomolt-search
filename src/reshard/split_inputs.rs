//! The input generations of a split: checked, replayed, and routed into
//! one replay per child.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::postings::{StoredBinding, StoredDerived};
use crate::wal::{self, WalManifest};

use super::child_build::{manifest_derived, ChildSchema};
use super::replay::{
    read_gen_manifest, read_gens_binding, replay_buckets, require_backend_config,
    require_complete_history, same_backend_config, Replay,
};
use super::WalCutoff;

/// The checked inputs of a split: the first input's manifest (the
/// children's provider state), the highest input generation, and the
/// binding and declaration every child keeps.
pub(super) struct SplitInputs {
    pub(super) manifest: WalManifest,
    pub(super) top_generation: u64,
    pub(super) binding: Option<StoredBinding>,
    pub(super) derived: Option<StoredDerived>,
}

impl SplitInputs {
    /// Every input must be full history under the first input's provider
    /// configuration and, with `same_geometry`, its bucket count. `what`
    /// names the split in the refusal of an empty input list.
    pub(super) fn check(gens: &[PathBuf], what: &str, same_geometry: bool) -> Result<Self, String> {
        let (manifest, top_generation) = check_manifests(gens, what, same_geometry)?;
        Ok(Self {
            derived: manifest_derived(&manifest)?,
            manifest,
            top_generation,
            binding: read_gens_binding(gens)?,
        })
    }

    /// The schema a split's children are written under: the given BM25
    /// field table, the inputs' binding, and the declaration the log's
    /// manifest records.
    pub(super) fn schema<'a>(&'a self, bm25_fields: Option<&'a [String]>) -> ChildSchema<'a> {
        ChildSchema {
            bm25_fields,
            binding: self.binding.as_ref(),
            derived: self.derived.as_ref(),
            ..ChildSchema::default()
        }
    }

    /// One past the highest input generation.
    pub(super) fn next_generation(&self) -> Result<u64, String> {
        self.top_generation
            .checked_add(1)
            .ok_or_else(|| "reshard generation overflow".to_string())
    }
}

/// The first input's manifest and the highest generation, once every
/// input is checked.
pub(super) fn check_manifests(
    gens: &[PathBuf],
    what: &str,
    same_geometry: bool,
) -> Result<(WalManifest, u64), String> {
    let Some(first_gen) = gens.first() else {
        return Err(format!("{what} requires at least one input generation"));
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input; split/merge \
                 requires one scoring configuration",
                gen.display()
            ));
        }
        if same_geometry && current.bucket_count != manifest.bucket_count {
            return Err(format!(
                "{}: bucket count {} differs from the first input's {}; split inputs must share \
                 one bucket geometry",
                gen.display(),
                current.bucket_count,
                manifest.bucket_count
            ));
        }
        top_generation = top_generation.max(current.generation);
    }
    Ok((manifest, top_generation))
}

/// Create a split's output directory.
pub(super) fn create_out_dir(out_dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))
}

/// Every input replayed whole into one replay, with the clock each
/// input's log had reached: a live catch-up resumes strictly after it.
pub(super) fn replay_clocked(
    gens: &[PathBuf],
    vectors_only: bool,
) -> Result<(Replay, Vec<WalCutoff>), String> {
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
    }
    replay.compact();
    Ok((replay, cutoffs))
}

/// Refuse a live row without a routed stable key: a stable split routes
/// by key, and legacy explicitly addressed rows have none.
pub(super) fn require_stable_keys(replay: &Replay) -> Result<(), String> {
    for id in replay.vectors.keys().chain(replay.documents.keys()) {
        if replay.stable_keys.get(id).is_none_or(|key| key.is_empty()) {
            return Err(format!(
                "stable split requires a routed stable key for live source id {id}; rebuild \
                 legacy explicitly addressed rows before a live split"
            ));
        }
    }
    Ok(())
}

/// Route every live row of `replay` to one of `n` children by `route`,
/// which sees the row's id and stable key; the row's vector, retained
/// payload, document, and key move with it.
pub(super) fn distribute(
    mut replay: Replay,
    n: usize,
    mut route: impl FnMut(u64, Option<&Vec<u8>>) -> Result<usize, String>,
) -> Result<Vec<Replay>, String> {
    let keys: BTreeMap<u64, Vec<u8>> = std::mem::take(&mut replay.stable_keys);
    let mut children: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
    for (id, vector) in replay.vectors {
        let key = keys.get(&id);
        let child = &mut children[route(id, key)?];
        child.vectors.insert(id, vector);
        if let Some(row) = replay.retained.remove(&id) {
            child.retained.insert(id, row);
        }
        if let Some(key) = key {
            child.stable_keys.insert(id, key.clone());
        }
    }
    for (id, document) in replay.documents {
        let key = keys.get(&id);
        let child = &mut children[route(id, key)?];
        child.documents.insert(id, document);
        if let Some(key) = key {
            child.stable_keys.insert(id, key.clone());
        }
    }
    Ok(children)
}

/// The inclusive hash range of child `i` of a `n`-way power-of-two split.
pub(super) fn hash_range(i: usize, n: usize) -> (u64, u64) {
    if n == 1 {
        return (0, u64::MAX);
    }
    let shift = 64 - n.trailing_zeros();
    let lo = (i as u64) << shift;
    let hi = if i + 1 == n {
        u64::MAX
    } else {
        ((i as u64 + 1) << shift) - 1
    };
    (lo, hi)
}
