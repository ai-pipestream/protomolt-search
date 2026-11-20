//! Retired single-image artifacts and corrupt catalogs refuse shard
//! open by name instead of being silently ignored or guessed past.

use std::path::PathBuf;

use pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::snapshot_repository::{RepositoryManifest, FORMAT_VERSION};

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protomolt_retired_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, wal: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        wal,
        wal_buckets: 2,
        ..Default::default()
    }
}

fn sibling(index_path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut candidate = index_path.as_os_str().to_owned();
    candidate.push(suffix);
    PathBuf::from(candidate)
}

#[test]
fn retired_artifacts_refuse_open_by_name() {
    for suffix in [".bm25", ".bm25.build", ".snap", ".snap-old"] {
        let dir = tempdir(&suffix.replace('.', "dot"));
        let index_path = dir.join("shard.tv");
        let retired = sibling(&index_path, suffix);
        if suffix == ".bm25.build" {
            std::fs::create_dir_all(&retired).unwrap();
        } else {
            std::fs::write(&retired, b"retired image bytes").unwrap();
        }
        let error = match NodeServiceImpl::open(config(index_path, false), None) {
            Ok(_) => panic!("{suffix} must refuse open"),
            Err(error) => error,
        };
        assert!(
            error.contains("retired single-image artifact"),
            "{suffix}: {error}"
        );
        assert!(error.contains(&retired.display().to_string()), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn repository_manifest_refuses_the_retired_layout() {
    let manifest = RepositoryManifest {
        format_version: FORMAT_VERSION,
        layout: "single-image".to_string(),
        backend_kind: String::new(),
        scoring_fingerprint: String::new(),
        dim: 0,
        slot_offset: 0,
        collection: String::new(),
        vector_rows: 0,
        document_rows: 0,
        live_rows: 0,
        analysis_fingerprints: Vec::new(),
        wal_generation: 0,
        wal_high_watermark: 0,
        wal_clocked: false,
        artifacts: Vec::new(),
    };
    let error = manifest.validate().unwrap_err();
    assert!(error.contains("single-image"), "{error}");
    assert!(error.contains("segments"), "{error}");
}

#[test]
fn corrupt_catalog_refuses_open_without_touching_the_wal() {
    let dir = tempdir("corrupt-catalog");
    let index_path = dir.join("shard.tv");
    drop(NodeServiceImpl::open(config(index_path.clone(), true), None).unwrap());
    let root = pipestream_search::node::segments_root(&index_path);
    std::fs::write(root.join("segments.json"), b"{not json").unwrap();
    let wal_dir = sibling(&index_path, ".wal");
    let wal_before: Vec<u64> = walk_sizes(&wal_dir);
    let error = match NodeServiceImpl::open(config(index_path, true), None) {
        Ok(_) => panic!("a corrupt segments.json must refuse open"),
        Err(error) => error,
    };
    assert!(!error.is_empty());
    assert_eq!(
        walk_sizes(&wal_dir),
        wal_before,
        "a refused open must not truncate or rewrite the WAL"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk_sizes(dir: &std::path::Path) -> Vec<u64> {
    let mut sizes = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                sizes.push(meta.len());
            }
        }
    }
    sizes.sort_unstable();
    sizes
}
