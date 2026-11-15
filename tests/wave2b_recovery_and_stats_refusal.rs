//! Regression tests for crash-recovery filesystem error propagation:
//! `recover_segments_swap`, `try_recover_generation`, and `NodeServiceImpl::open`
//! must propagate real filesystem errors and fail closed rather than opening an
//! empty or half-swapped shard when `remove_dir_all` or `rename` fails.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use pipestream_search::node::{
    generation_bm25, generation_dir, segments_root, Layout, NodeConfig, NodeServiceImpl,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store, DocTerms};
use pipestream_search::segments::{SegmentCatalog, SegmentSetManifest};

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("recovery_io_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn is_effective_root() -> bool {
    // SAFETY: `geteuid` is thread-safe and takes no arguments.
    unsafe { libc::geteuid() == 0 }
}

fn write_single_doc_bm25(path: &Path, text: &str) {
    let mut store = Bm25Store::new();
    let mut terms: DocTerms = Vec::new();
    let mut offset = 0u32;
    for token in text.split_whitespace() {
        let start = text[offset as usize..].find(token).unwrap() as u32 + offset;
        terms.push((
            token.to_string(),
            1,
            vec![(start, start + token.len() as u32)],
        ));
        offset = start + token.len() as u32;
    }
    let length = terms.iter().map(|(_, freq, _)| freq).sum();
    store.add_document_with_lineage(0, text.to_string(), AnalyzedDoc::body(terms, length), None);
    store
        .source_archive_mut()
        .attach_source(0, &common::protobuf_source(text, "recovery"), None)
        .unwrap();
    store.save(path).unwrap();
}

#[cfg(unix)]
#[test]
fn segments_swap_cleanup_io_error_refuses_shard_open() {
    let dir = tempdir("segments_swap_cleanup_err");
    let index_path = dir.join("shard.idx");
    let root = segments_root(&index_path);
    let old = dir.join("shard.idx.segments.snap-old");
    std::fs::create_dir_all(&root).unwrap();
    let manifest_bytes = serde_json::to_vec_pretty(&SegmentSetManifest::default()).unwrap();
    std::fs::write(SegmentCatalog::manifest_path(&root), &manifest_bytes).unwrap();

    // A regular file at `.segments.snap-old` makes `remove_dir_all(&old)` fail
    // deterministically with `ENOTDIR` (even under UID 0), requiring `open` to
    // surface the cleanup error and refuse shard initialization.
    std::fs::write(&old, b"corrupt-non-dir-backup").unwrap();

    let opened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path.clone()),
            layout: Layout::Segments,
            ..Default::default()
        },
        None,
        true,
    );
    assert!(
        opened.is_err(),
        "NodeServiceImpl::open must fail closed when retiring `.segments.snap-old` fails"
    );

    // Likewise, a stray `.snap-export-*` regular file that cannot be retired by
    // `remove_dir_all` must surface its I/O error instead of being ignored.
    std::fs::remove_file(&old).unwrap();
    let stray_export = dir.join("shard.idx.snap-export-orphan");
    std::fs::write(&stray_export, b"corrupt-export-entry").unwrap();
    let opened_export = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path),
            layout: Layout::Segments,
            ..Default::default()
        },
        None,
        true,
    );
    assert!(
        opened_export.is_err(),
        "NodeServiceImpl::open must fail closed when cleaning `.snap-export-*` fails"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn segments_swap_rollback_rename_error_refuses_shard_open() {
    if is_effective_root() {
        return;
    }
    let dir = tempdir("segments_swap_rollback_err");
    let index_path = dir.join("shard.idx");
    let root = segments_root(&index_path);
    let old = dir.join("shard.idx.segments.snap-old");
    std::fs::create_dir_all(&old).unwrap();
    let manifest_bytes = serde_json::to_vec_pretty(&SegmentSetManifest::default()).unwrap();
    std::fs::write(SegmentCatalog::manifest_path(&old), &manifest_bytes).unwrap();

    // Incomplete `.segments` directory with no `segments.json` and a read-only
    // child directory so `remove_dir_all(&root)` fails with `EACCES` and
    // `rename(&old, &root)` fails with `ENOTEMPTY`. Swallowing those errors
    // opens an empty shard while stranding the catalog in `.segments.snap-old`.
    let locked = root.join("locked");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::write(locked.join("pin"), b"pinned").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

    let opened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path),
            layout: Layout::Segments,
            ..Default::default()
        },
        None,
        true,
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        opened.is_err(),
        "NodeServiceImpl::open must fail closed when segments swap rollback fails on disk"
    );
    assert!(
        SegmentCatalog::manifest_path(&old).exists(),
        "backup segment catalog manifest must remain intact when rollback fails"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn generation_swap_cleanup_and_rollback_io_errors_refuse_shard_open() {
    let dir = tempdir("generation_swap_err");
    let index_path = dir.join("shard.idx");
    let snap = generation_dir(&index_path);
    let old = dir.join("shard.idx.snap-old");
    let tmp = dir.join("shard.idx.snap-tmp");

    std::fs::create_dir_all(&old).unwrap();
    write_single_doc_bm25(&generation_bm25(&old), "court opinion preserved in backup");
    // `.snap-tmp` as a regular file causes `remove_dir_all(&tmp)` to fail
    // deterministically with `ENOTDIR` even under UID 0, before rollback runs.
    std::fs::write(&tmp, b"corrupt-non-dir-staging").unwrap();

    let opened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path.clone()),
            ..Default::default()
        },
        None,
        true,
    );
    assert!(
        opened.is_err(),
        "NodeServiceImpl::open must fail closed when generation recovery cleanup fails"
    );
    assert!(
        generation_bm25(&old).exists(),
        "backup generation must remain intact when pre-rollback cleanup fails"
    );
    assert!(!snap.exists());
    std::fs::remove_file(&tmp).unwrap();

    if !is_effective_root() {
        // Make the parent directory read-only (`0o555`) while `.snap` is absent and
        // `.snap-old` holds the shard's generation so `rename(&old, &snap)` fails
        // with `EACCES`; `open` must refuse rather than opening an empty shard with
        // its installed generation stranded in `.snap-old`.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let rollback_opened = NodeServiceImpl::open(
            NodeConfig {
                index_path: Some(index_path),
                ..Default::default()
            },
            None,
            true,
        );
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            rollback_opened.is_err(),
            "NodeServiceImpl::open must fail closed when `rename(&old, &snap)` fails"
        );
        assert!(
            generation_bm25(&old).exists(),
            "backup generation must remain intact when rollback rename fails"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
