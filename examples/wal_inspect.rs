//! Read-only WAL inspector: prints what a shard's write-ahead log
//! contains without touching it — the tool you want mid-ingest and
//! before any reshard.
//!
//! ```text
//! # Every generation in a shard's WAL directory:
//! wal_inspect --wal=/data/shard-0.tv.wal
//!
//! # One generation, with every record:
//! wal_inspect --wal=/data/shard-0.tv.wal/gen-000001 --records
//! ```
//!
//! Per generation it reports the manifest (shape, calibration presence,
//! preexisting state — the field that decides whether the reshard tool
//! will accept the log), then per bucket file: record count, seq range,
//! id range, bytes, and whether a torn tail is present. Markers are
//! decoded. Exit code is 0 even for torn tails (they are a normal crash
//! artifact the node truncates on resume); undecodable frames and seq
//! gaps exit nonzero, because those mean real corruption.

use std::path::{Path, PathBuf};

use turbovec_search::pb::wal::wal_record;
use turbovec_search::wal::{self, RecordReader};

fn opt(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn flag(key: &str) -> bool {
    let name = format!("--{key}");
    std::env::args().any(|a| a == name)
}

/// Inspect one bucket or markers file; returns (records, bytes_valid).
fn inspect_file(path: &Path, records: bool) -> Result<(u64, u64), String> {
    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .len();
    let mut reader = RecordReader::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut count = 0u64;
    let mut first_seq = 0u64;
    let mut last_seq = 0u64;
    let mut id_lo = u64::MAX;
    let mut id_hi = 0u64;
    let mut valid_len = 0u64;
    while let Some(record) = reader
        .next_record()
        .map_err(|e| format!("{}: {e}", path.display()))?
    {
        count += 1;
        if first_seq == 0 {
            first_seq = record.seq;
        }
        last_seq = record.seq;
        valid_len = reader.offset();
        let described = match &record.op {
            Some(wal_record::Op::AddVectors(a)) => {
                id_lo = id_lo.min(a.first_id);
                id_hi = id_hi.max(a.first_id);
                format!("add_vectors id={}", a.first_id)
            }
            Some(wal_record::Op::AddDocuments(a)) => {
                id_lo = id_lo.min(a.first_id);
                id_hi = id_hi.max(a.first_id);
                format!("add_documents id={} n={}", a.first_id, a.documents.len())
            }
            Some(wal_record::Op::Bind(b)) => {
                format!("bind plan={} body={:?}", b.plan_fingerprint, b.body_path)
            }
            Some(wal_record::Op::Flush(_)) => "flush".to_string(),
            Some(wal_record::Op::Snapshot(s)) => {
                format!("snapshot source_generation={}", s.source_generation)
            }
            None => "EMPTY OP".to_string(),
        };
        if records {
            println!("    seq={:>6} {described}", record.seq);
        }
    }
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let torn = if valid_len < file_len {
        format!("  TORN TAIL ({} byte(s))", file_len - valid_len)
    } else {
        String::new()
    };
    let ids = if id_lo <= id_hi {
        format!(" ids {id_lo}..={id_hi}")
    } else {
        String::new()
    };
    println!(
        "  {name}: {count} record(s), seq {first_seq}..={last_seq},{ids} {file_len} byte(s){torn}"
    );
    Ok((count, file_len))
}

fn inspect_gen(gen: &Path, records: bool) -> Result<(), String> {
    let manifest = wal::read_manifest(gen).map_err(|e| format!("{}: {e}", gen.display()))?;
    let calibrated = !manifest.calibration_shift.is_empty();
    println!("{}", gen.display());
    println!(
        "  manifest: dim={} bit_width={} slot_offset={} generation={} buckets={} \
         calibration={} format v{}",
        manifest.dim,
        manifest.bit_width,
        manifest.slot_offset,
        manifest.generation,
        manifest.bucket_count,
        if calibrated {
            "locked"
        } else {
            "NONE (unseeded; not reshardable)"
        },
        manifest.format_version,
    );
    if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
        println!(
            "  PARTIAL HISTORY: {} vector(s) / {} document(s) predate this log; \
             the reshard tool will refuse it",
            manifest.preexisting_vectors, manifest.preexisting_documents
        );
    }
    let mut names: Vec<PathBuf> = std::fs::read_dir(gen)
        .map_err(|e| format!("{}: {e}", gen.display()))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            path.extension().is_some_and(|x| x == "wal").then_some(path)
        })
        .collect();
    names.sort();
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;
    for path in names {
        let (count, bytes) = inspect_file(&path, records)?;
        total_records += count;
        total_bytes += bytes;
    }
    println!("  total: {total_records} record(s), {total_bytes} byte(s)");
    Ok(())
}

fn main() -> Result<(), String> {
    let Some(target) = opt("wal") else {
        return Err("usage: wal_inspect --wal=<wal dir | generation dir> [--records]".to_string());
    };
    let records = flag("records");
    let target = PathBuf::from(target);
    if wal::manifest_path(&target).exists() {
        return inspect_gen(&target, records);
    }
    // A WAL directory: every generation, oldest first, including any
    // retired `.broken` directories (reported, never opened as a log).
    let mut gens: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(&target).map_err(|e| format!("{}: {e}", target.display()))? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if name.ends_with(".broken") {
            println!(
                "{}: retired as broken (append failure); not a usable log",
                path.display()
            );
        } else if name.starts_with("gen-") && path.is_dir() {
            gens.push(path);
        }
    }
    if gens.is_empty() {
        return Err(format!("no WAL generations in {}", target.display()));
    }
    gens.sort();
    for gen in gens {
        inspect_gen(&gen, records)?;
    }
    Ok(())
}
