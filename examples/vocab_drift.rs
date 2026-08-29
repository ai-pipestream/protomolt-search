//! Read-only vocabulary-snapshot tool: list a shard's snapshot history,
//! print the drift between two of them, or merge snapshots across shards
//! (the coordinator-aggregation primitive). Never writes into the vocab
//! directory — the tool you want mid-ingest.
//!
//! ```text
//! # Every snapshot in a shard's vocab directory:
//! vocab_drift --dir=/data/shard-0.tv.vocab
//!
//! # Drift between two windows (bare sequence numbers or file names):
//! vocab_drift --dir=/data/shard-0.tv.vocab --from=0 --to=3
//!
//! # ... including embedding-OOV coverage of the TOKENS channel:
//! vocab_drift --dir=/data/shard-0.tv.vocab --from=0 --to=3 --embeddings=/models/potion
//!
//! # Merge shard-level snapshots into one aggregate (written OUTSIDE the
//! # scanned directory unless you say otherwise):
//! vocab_drift --dir=/data/shard-0.tv.vocab --merge=0,1,2 --out=/tmp/merged.pb
//! ```
//!
//! Per channel the drift report shows both windows' distinct-term
//! cardinalities and their union, the novelty rate (share of the newer
//! window's vocabulary the older never saw), the Jensen-Shannon divergence
//! over the union of the heavy-hitter lists, and — with `--embeddings` —
//! the share of the newer window's token mass the model's vocabulary does
//! not cover. Exit code is nonzero on unknown references or unreadable
//! snapshots, zero otherwise (drift itself is information, not failure).

use std::path::{Path, PathBuf};

use pipestream_search::vocab::{self, SnapshotMeta};
use prost::Message;

fn opt(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn usage() -> ! {
    eprintln!(
        "usage: vocab_drift --dir=<vocab dir> [--from=<ref> --to=<ref>] [--embeddings=<model dir>]\n\
         \x20      vocab_drift --dir=<vocab dir> --merge=<seq,seq,...> --out=<file.pb>\n\
         \x20      (a reference is a bare sequence number or a snapshot file name)"
    );
    std::process::exit(2);
}

/// Resolve a reference (bare sequence or file name) against the scan.
fn resolve<'a>(scan: &'a [(SnapshotMeta, PathBuf)], reference: &str) -> Result<&'a Path, String> {
    if let Ok(sequence) = reference.parse::<u64>() {
        if let Some((_, path)) = scan.iter().find(|(meta, _)| meta.sequence == sequence) {
            return Ok(path);
        }
    }
    scan.iter()
        .find(|(meta, _)| meta.name == reference)
        .map(|(_, path)| path.as_path())
        .ok_or_else(|| format!("unknown snapshot '{reference}'"))
}

fn channel_name(channel: i32) -> &'static str {
    match pipestream_search::pb::analysis::VocabChannel::try_from(channel) {
        Ok(pipestream_search::pb::analysis::VocabChannel::Terms) => "TERMS ",
        Ok(pipestream_search::pb::analysis::VocabChannel::Tokens) => "TOKENS",
        _ => "????? ",
    }
}

fn main() {
    let Some(dir) = opt("dir") else {
        usage();
    };
    let dir = PathBuf::from(dir);
    let scan = vocab::scan_snapshot_dir(&dir).unwrap_or_else(|e| {
        eprintln!("vocab_drift: scan {}: {e}", dir.display());
        std::process::exit(1);
    });
    if scan.is_empty() {
        println!("{}: no snapshots", dir.display());
        return;
    }

    if let Some(merge) = opt("merge") {
        let Some(out) = opt("out") else {
            usage();
        };
        let mut snapshots = Vec::new();
        for reference in merge.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let path = resolve(&scan, reference).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            });
            snapshots.push(vocab::load_snapshot_file(path).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            }));
        }
        if snapshots.len() < 2 {
            eprintln!("vocab_drift: --merge needs at least two snapshots");
            std::process::exit(2);
        }
        let merged = vocab::merge_snapshots(&snapshots).unwrap_or_else(|e| {
            eprintln!("vocab_drift: merge: {e}");
            std::process::exit(1);
        });
        let bytes = merged.encode_to_vec();
        std::fs::write(&out, &bytes).unwrap_or_else(|e| {
            eprintln!("vocab_drift: write {out}: {e}");
            std::process::exit(1);
        });
        let documents: u64 = merged
            .channels
            .iter()
            .map(|c| c.documents)
            .max()
            .unwrap_or(0);
        println!(
            "merged {} snapshot(s) -> {out} ({} byte(s), {} document(s))",
            snapshots.len(),
            bytes.len(),
            documents
        );
        return;
    }

    match (opt("from"), opt("to")) {
        (None, None) => {
            println!("{}: {} snapshot(s)", dir.display(), scan.len());
            for (meta, _) in &scan {
                println!(
                    "  seq={:<4} docs={:<10} {} .. {} ({} byte(s))  {}",
                    meta.sequence,
                    meta.documents,
                    meta.started_epoch_millis,
                    meta.sealed_epoch_millis,
                    meta.size_bytes,
                    meta.name
                );
            }
        }
        (Some(from_ref), Some(to_ref)) => {
            let from_path = resolve(&scan, &from_ref).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            });
            let to_path = resolve(&scan, &to_ref).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            });
            let from = vocab::load_snapshot_file(from_path).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            });
            let to = vocab::load_snapshot_file(to_path).unwrap_or_else(|e| {
                eprintln!("vocab_drift: {e}");
                std::process::exit(1);
            });
            let embeddings = opt("embeddings").map(PathBuf::from);
            let vocabulary = embeddings
                .as_deref()
                .and_then(vocab::load_embedding_vocabulary);
            if embeddings.is_some() && vocabulary.is_none() {
                eprintln!(
                    "vocab_drift: no readable embedding vocabulary (vocab.txt / tokenizer.json); \
                     OOV share not computed"
                );
            }
            println!("from: {}", from_path.display());
            println!("to:   {}", to_path.display());
            let drift = vocab::compute_channel_drift(&from, &to, vocabulary.as_ref())
                .unwrap_or_else(|e| {
                    eprintln!("vocab_drift: {e}");
                    std::process::exit(1);
                });
            for channel in &drift {
                let m = &channel.metrics;
                println!(
                    "  {} cardinality from={:.0} to={:.0} union={:.0}  novelty={:.4}  \
                     js-divergence={:.4}",
                    channel_name(channel.channel as i32),
                    m.from_cardinality,
                    m.to_cardinality,
                    m.union_cardinality,
                    m.novelty_rate,
                    m.jensen_shannon_divergence
                );
                if channel.channel == pipestream_search::pb::analysis::VocabChannel::Tokens {
                    if m.embedding_oov_computed {
                        println!(
                            "        embedding OOV share (token mass): {:.4}",
                            m.embedding_oov_share
                        );
                    } else {
                        println!("        embedding OOV share: not computed (no vocabulary given/readable)");
                    }
                }
            }
        }
        _ => usage(),
    }
}
