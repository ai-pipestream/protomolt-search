use super::*;
use crate::pb::{DocumentIdentity, ProtobufSource};
use crate::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
use crate::test_support::ForkGuarded;

/// Net allocation on the calling thread, for the proof's own thread:
/// the proof allocates and frees on that thread, and the engine's
/// per-block conversions stay below its parallel threshold.
mod allocation;
mod fixture;
mod memory_tests;
mod rejection_tests;

use fixture::Fixture;

#[test]
fn rewrite_proof_rejects_lost_or_changed_lattice_and_splade_payloads() {
    let fixture = Fixture::new();
    let before = fixture.set("original", &[0, 1, 2], &[], "", 2);
    for change in [
        "pos_length",
        "missing_splade",
        "splade_impact",
        "splade_scale",
    ] {
        let after = fixture.set(change, &[2, 1, 0], &[], change, 3);
        assert!(
            before
                .verify_source_rewrite(&after, &fixture.0.join("proof"), 1)
                .is_err(),
            "accepted {change}"
        );
    }
}

#[test]
fn final_segment_can_be_reclaimed_only_with_identical_generation_metadata() {
    let fixture = Fixture::new();
    let before = fixture.set("all-deleted", &[0, 1], &[0, 1], "", 2);
    let declaration = generation::from_reader(before.bm25(0), backend(&before).unwrap()).unwrap();
    let expected = before
        .verify_source_rewrite(&before, &fixture.0.join("self-proof"), 1)
        .unwrap();
    let mut manifest = before.published_manifest();
    manifest.format = 4;
    manifest.generation_declaration =
        Some(SegmentGenerationDeclaration::encode(&declaration).unwrap());
    manifest.segments.clear();
    manifest.epoch += 1;
    let empty = OpenedSegmentSet::open_manifest(
        before.root().to_path_buf(),
        manifest.clone(),
        Default::default(),
    )
    .unwrap();
    let proof = before
        .verify_source_rewrite(&empty, &fixture.0.join("empty-proof"), 1)
        .unwrap();
    assert_eq!(proof, expected);
    assert_eq!(proof.live_rows, 0);
    for change in 0..3 {
        let mut altered = declaration.clone();
        match change {
            0 => altered.columns.as_mut().unwrap().facets.pop(),
            1 => {
                altered.text_fields[0].analysis_fingerprint = Some(12);
                None
            }
            2 => {
                altered.vector = None;
                None
            }
            _ => unreachable!(),
        };
        manifest.generation_declaration =
            Some(SegmentGenerationDeclaration::encode(&altered).unwrap());
        let mut populated = manifest.clone();
        populated.segments = before.manifest().segments.clone();
        assert!(OpenedSegmentSet::open_manifest(
            before.root().to_path_buf(),
            populated,
            Default::default()
        )
        .is_err());
        let changed = OpenedSegmentSet::open_manifest(
            before.root().to_path_buf(),
            manifest.clone(),
            Default::default(),
        )
        .unwrap();
        assert!(before
            .verify_source_rewrite(&changed, &fixture.0.join("bad-proof"), 1)
            .is_err());
    }
}

#[test]
fn rewrite_proof_preserves_identity_across_physical_reordering_and_tombstone_reclamation() {
    let fixture = Fixture::new();
    let before = fixture.set("before", &[0, 1, 2, 3], &[0], "", 2);
    let after = fixture.set("after", &[3, 1, 2], &[], "", 3);
    let mut expected = None;
    for batch in [1, 2, 1024] {
        let scratch = fixture.0.join(format!("scratch-{batch}"));
        let proof = before
            .verify_source_rewrite(&after, &scratch, batch)
            .unwrap();
        assert_eq!(proof.live_rows, 3);
        if let Some(expected) = &expected {
            assert_eq!(expected, &proof);
        } else {
            expected = Some(proof);
        }
        assert!(!scratch.exists());
    }
}

#[test]
/// Equal exact rows and equal configuration do not certify a dense image
/// whose stored codes came from other vectors. The same rows encoded
/// unchanged, or in another order and cut, still certify.
fn rewrite_proof_rejects_quantized_image_built_from_different_rows() {
    let fixture = Fixture::new();
    let before = fixture.set("before", &[0, 1], &[], "", 2);
    let after = fixture.set_with_quantized_rows("after", &[0, 1], &[], "", 2, true);
    let scratch = fixture.0.join("proof");
    let error = before
        .verify_source_rewrite(&after, &scratch, 1)
        .unwrap_err();
    assert!(error.contains("encoded dense row"), "{error}");
    assert!(!scratch.exists());

    let same = fixture.set_with_quantized_rows("same", &[0, 1], &[], "", 2, false);
    let proof = before.verify_source_rewrite(&same, &scratch, 1).unwrap();
    assert_eq!(proof.format_version, CERTIFICATE_FORMAT);
    assert_eq!(proof.live_rows, 2);
    let reordered = fixture.set_with_quantized_rows("reordered", &[1, 0], &[], "", 1, false);
    assert_eq!(
        before
            .verify_source_rewrite(&reordered, &scratch, 1)
            .unwrap(),
        proof
    );
    let perturbed_reordered =
        fixture.set_with_quantized_rows("perturbed-reordered", &[1, 0], &[], "", 1, true);
    assert!(before
        .verify_source_rewrite(&perturbed_reordered, &scratch, 1)
        .is_err());
}

/// The batch bounds digest memory and nothing else: the certificate is
/// identical at every size, and the vocabulary is walked once per
/// (segment, field, batch), so passes = fields * sum(ceil(rows / batch)).
#[test]
fn vocabulary_passes_scale_with_batches_and_the_certificate_does_not() {
    let fixture = Fixture::new();
    let ids: Vec<u32> = (0..280).collect();
    let before = fixture.set("before", &ids, &[], "", 280);
    let after = fixture.set("after", &ids, &[], "", 100);
    let fields = 2u64;
    let segments = |cut: u64| {
        (0..280u64)
            .step_by(cut as usize)
            .map(move |s| (280 - s).min(cut))
    };
    let expected_passes = |batch: u64| -> u64 {
        let before: u64 = segments(280).map(|rows| rows.div_ceil(batch)).sum();
        let after: u64 = segments(100).map(|rows| rows.div_ceil(batch)).sum();
        fields * (before + after)
    };
    let mut certificate = None;
    let mut measured = Vec::new();
    for batch in [1u64, 7, 64, 280, 1024, MAX_BATCH_ROWS as u64] {
        VOCABULARY_PASSES.with(|passes| passes.set(0));
        let proof = before
            .verify_source_rewrite(&after, &fixture.0.join("scratch"), batch as usize)
            .unwrap();
        let passes = VOCABULARY_PASSES.with(|passes| passes.get());
        assert_eq!(passes, expected_passes(batch), "batch {batch}");
        measured.push((batch, passes));
        match &certificate {
            Some(expected) => assert_eq!(expected, &proof, "batch {batch}"),
            None => certificate = Some(proof),
        }
    }
    eprintln!("vocabulary passes by batch (280 rows, 2 fields, 1 + 3 segments): {measured:?}");
    assert!(proof_batch_rows_for_budget(0).is_err());
    assert!(proof_batch_rows_for_budget(31).is_err());
    assert_eq!(proof_batch_rows_for_budget(32).unwrap(), 1);
    assert_eq!(proof_batch_rows_for_budget(63).unwrap(), 1);
    assert_eq!(proof_batch_rows_for_budget(64).unwrap(), 2);
    assert_eq!(proof_batch_rows_for_budget(32 * 1000).unwrap(), 1000);
    assert_eq!(
        proof_batch_rows_for_budget(u64::MAX).unwrap(),
        MAX_BATCH_ROWS
    );
    assert_eq!(DEFAULT_PROOF_BATCH_ROWS, MAX_BATCH_ROWS);
}

/// The scratch is private from creation. The check runs in a child
/// process whose umask permits everything, so the modes observed are
/// the ones requested at creation, not the process default.
#[cfg(unix)]
#[test]
fn scratch_is_private_under_a_permissive_umask() {
    use std::os::unix::fs::PermissionsExt;
    const CHILD: &str = "REWRITE_PROOF_UMASK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // SAFETY: the child process is single-purpose; nothing else in it
        // depends on the creation mask.
        unsafe { libc::umask(0) };
        let fixture = Fixture::new();
        let before = fixture.set("before", &[0, 1], &[], "", 2);
        let observed = std::rc::Rc::new(std::cell::Cell::new(false));
        let seen = observed.clone();
        AFTER_SCRATCH_CREATED.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |scratch: &Path| {
                let mode = |path: &Path| {
                    std::fs::symlink_metadata(path)
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o7777
                };
                assert_eq!(mode(scratch), 0o700, "scratch directory mode");
                assert_eq!(
                    mode(&scratch.join("rows.redb")),
                    0o600,
                    "scratch database mode"
                );
                seen.set(true);
            }));
        });
        before
            .verify_source_rewrite(&before, &fixture.0.join("proof"), 1)
            .unwrap();
        assert!(observed.get(), "the scratch observer did not run");
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "segments::rewrite_proof::tests::scratch_is_private_under_a_permissive_umask",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .status_guarded()
        .unwrap();
    assert!(status.success(), "child exited with {status}");
}
