//! Source-tree hygiene: a per-file line ceiling over every `.rs` file
//! under `src/` and `crates/` (issue #119, Epic #120).
//!
//! A non-test source file stays at or under `DEFAULT_CEILING` lines and a
//! test file (a name ending in `tests.rs`) at or under `TESTS_CEILING`.
//! Files that were already past the default when the rule landed are
//! listed in `RATCHET`, each with its own ceiling: its size that day
//! plus `RATCHET_HEADROOM` lines for active feature work. The table
//! only shrinks. An entry whose file is gone, or has been split down to
//! the default ceiling, fails this test until the entry is removed, so a
//! modularized file cannot grow back past the default behind a stale
//! allowance.
//!
//! A new file never gets an entry: split it instead.

use std::fs;
use std::path::{Path, PathBuf};

/// The ceiling for a non-test `.rs` file.
const DEFAULT_CEILING: usize = 1_500;
/// The ceiling for a `*tests.rs` file.
const TESTS_CEILING: usize = 2_200;
/// The growth each ratcheted file was allowed over its size on the day
/// the table was written.
const RATCHET_HEADROOM: usize = 150;

/// Files above the default ceiling when the rule landed, with their
/// ceilings (size that day plus `RATCHET_HEADROOM`). Lower an entry when
/// its file shrinks; remove it when the file is split or deleted.
const RATCHET: &[(&str, usize)] = &[
    ("src/node.rs", 18_743 + RATCHET_HEADROOM),
    ("src/coordinator.rs", 17_308 + RATCHET_HEADROOM),
    ("src/postings.rs", 13_230 + RATCHET_HEADROOM),
    ("src/reshard.rs", 6_221 + RATCHET_HEADROOM),
    ("src/relay.rs", 5_872 + RATCHET_HEADROOM),
    ("src/query.rs", 2_924 + RATCHET_HEADROOM),
    ("src/segments.rs", 2_867 + RATCHET_HEADROOM),
    ("src/leann_graph.rs", 2_799 + RATCHET_HEADROOM),
    ("src/mapping.rs", 2_695 + RATCHET_HEADROOM),
    ("src/cel.rs", 2_650 + RATCHET_HEADROOM),
    ("src/bm25.rs", 2_487 + RATCHET_HEADROOM),
    ("src/wal.rs", 2_460 + RATCHET_HEADROOM),
    ("src/values.rs", 2_117 + RATCHET_HEADROOM),
    ("src/analyzer.rs", 2_109 + RATCHET_HEADROOM),
    ("src/segmented.rs", 1_970 + RATCHET_HEADROOM),
    ("src/node_agent.rs", 1_964 + RATCHET_HEADROOM),
    ("src/vocab.rs", 1_930 + RATCHET_HEADROOM),
    ("src/main.rs", 1_882 + RATCHET_HEADROOM),
    ("src/placement.rs", 1_785 + RATCHET_HEADROOM),
    ("src/metrics.rs", 1_773 + RATCHET_HEADROOM),
    ("src/source_authority/import.rs", 1_772 + RATCHET_HEADROOM),
    ("src/source_authority.rs", 1_646 + RATCHET_HEADROOM),
    ("src/chunked.rs", 1_626 + RATCHET_HEADROOM),
    ("src/raft/transport.rs", 1_619 + RATCHET_HEADROOM),
    ("src/source_authority/capacity.rs", 1_542 + RATCHET_HEADROOM),
    ("src/raft/host.rs", 1_537 + RATCHET_HEADROOM),
    (
        "crates/protomolt-search-embedded/src/mobile.rs",
        1_505 + RATCHET_HEADROOM,
    ),
];

/// The directories walked, relative to the package root.
const ROOTS: &[&str] = &["src", "crates"];

fn is_tests_file(relative: &str) -> bool {
    relative.ends_with("tests.rs")
}

fn default_ceiling(relative: &str) -> usize {
    if is_tests_file(relative) {
        TESTS_CEILING
    } else {
        DEFAULT_CEILING
    }
}

/// Every `.rs` file under `dir`, skipping build output.
fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        let path = entry.path();
        let kind = entry
            .file_type()
            .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()));
        if kind.is_dir() {
            if entry.file_name() == "target" {
                continue;
            }
            collect(&path, out);
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// A path relative to the package root with `/` separators, the form
/// the table uses.
fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} is outside {}", path.display(), root.display()))
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn line_count(path: &Path) -> usize {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let text =
        String::from_utf8(bytes).unwrap_or_else(|e| panic!("{} is not UTF-8: {e}", path.display()));
    text.lines().count()
}

#[test]
fn ratchet_table_is_well_formed() {
    for (i, (path, ceiling)) in RATCHET.iter().enumerate() {
        assert!(
            *ceiling > default_ceiling(path),
            "{path}: a ratchet ceiling of {ceiling} is no allowance over the default \
             {}; remove the entry",
            default_ceiling(path)
        );
        assert!(
            RATCHET[..i].iter().all(|(other, _)| other != path),
            "{path} is listed twice in RATCHET"
        );
    }
}

#[test]
fn source_files_stay_under_their_line_ceilings() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in ROOTS {
        collect(&root.join(dir), &mut files);
    }
    files.sort();
    assert!(!files.is_empty(), "no .rs files found under {ROOTS:?}");

    let mut failures = Vec::new();
    let mut seen = Vec::new();
    for path in &files {
        let rel = relative(&root, path);
        let lines = line_count(path);
        let ratchet = RATCHET.iter().find(|(p, _)| *p == rel).map(|(_, c)| *c);
        match ratchet {
            Some(ceiling) => {
                seen.push(rel.clone());
                if lines <= default_ceiling(&rel) {
                    failures.push(format!(
                        "{rel}: {lines} lines is within the default ceiling of {}; \
                         remove its RATCHET entry",
                        default_ceiling(&rel)
                    ));
                } else if lines > ceiling {
                    failures.push(format!(
                        "{rel}: {lines} lines exceeds its ratcheted ceiling of {ceiling}; \
                         split it into focused submodules"
                    ));
                }
            }
            None => {
                let ceiling = default_ceiling(&rel);
                if lines > ceiling {
                    failures.push(format!(
                        "{rel}: {lines} lines exceeds the ceiling of {ceiling}; \
                         split it into focused submodules (new files get no RATCHET entry)"
                    ));
                }
            }
        }
    }
    for (path, _) in RATCHET {
        if !seen.iter().any(|s| s == path) {
            failures.push(format!(
                "{path}: listed in RATCHET but no longer exists; remove its entry"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "tree hygiene failures:\n  {}",
        failures.join("\n  ")
    );
}
