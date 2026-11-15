//! Multi-input reshard checks: one configuration and one derived declaration before any replay.

use std::path::{Path, PathBuf};

use crate::wal::WalManifest;

use super::child_build::manifest_derived;
use super::replay::{
    read_gen_manifest, require_backend_config, require_complete_history, same_backend_config,
};

/// The manifests of a merge's inputs, in input order. Every input shares
/// the first's provider configuration, bucket count, and derived-column
/// declaration, or the merge is refused naming the input that differs.
pub(super) fn load_merge_manifests(gens: &[PathBuf]) -> Result<Vec<WalManifest>, String> {
    let mut manifests: Vec<WalManifest> = Vec::with_capacity(gens.len());
    for gen in gens {
        let manifest = read_gen_manifest(gen)?;
        require_backend_config(&manifest, gen)?;
        require_complete_history(&manifest, gen)?;
        manifest_derived(&manifest).map_err(|e| format!("{}: {e}", gen.display()))?;
        if let Some(first) = manifests.first() {
            require_same_inputs(first, &gens[0], &manifest, gen)?;
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

/// One scoring configuration, one bucket geometry, one meaning for the
/// derived values: a row computed under one declaration is never
/// published under another, whichever input comes first.
fn require_same_inputs(
    first: &WalManifest,
    first_gen: &Path,
    manifest: &WalManifest,
    gen: &Path,
) -> Result<(), String> {
    if !same_backend_config(first, manifest) {
        return Err(format!(
            "{}: vector backend configuration differs from the first input; \
             split/merge requires one scoring configuration",
            gen.display()
        ));
    }
    if first.bucket_count != manifest.bucket_count {
        return Err(format!(
            "{}: bucket count {} differs from the first input's {}; merge inputs \
             must share one bucket geometry",
            gen.display(),
            manifest.bucket_count,
            first.bucket_count
        ));
    }
    require_same_declaration(first, first_gen, manifest, gen)?;
    Ok(())
}

/// Every input generation records one derived-column declaration, or
/// none: a child is stamped with the first input's, so an input under
/// another would be republished under a meaning it was not computed with.
pub(super) fn require_one_declaration(gens: &[PathBuf]) -> Result<(), String> {
    let mut first: Option<WalManifest> = None;
    for gen in gens {
        let manifest = read_gen_manifest(gen)?;
        manifest_derived(&manifest).map_err(|e| format!("{}: {e}", gen.display()))?;
        match &first {
            Some(first) => require_same_declaration(first, &gens[0], &manifest, gen)?,
            None => first = Some(manifest),
        }
    }
    Ok(())
}

fn require_same_declaration(
    first: &WalManifest,
    first_gen: &Path,
    manifest: &WalManifest,
    gen: &Path,
) -> Result<(), String> {
    if first.derived_fingerprint == manifest.derived_fingerprint {
        return Ok(());
    }
    Err(format!(
        "{} records derived-column declaration {} and {} records {}; inputs must share one \
         declaration, or none",
        first_gen.display(),
        declaration_name(first),
        gen.display(),
        declaration_name(manifest)
    ))
}

fn declaration_name(manifest: &WalManifest) -> String {
    if manifest.derived_fingerprint.is_empty() {
        "none".to_string()
    } else {
        format!("{:?}", manifest.derived_fingerprint)
    }
}
