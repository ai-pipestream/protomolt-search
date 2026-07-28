//! Court corpus prep: stream a CourtListener bulk opinions CSV (optionally
//! bz2-compressed) and write the NDJSON `{id, cluster_id, plain_text}`
//! sample the chunker (`court_chunks`) consumes.
//!
//! Replaces the old Python one-pass extractor. Row semantics:
//!
//! - rows are PostgreSQL COPY CSV (FORCE_QUOTE *, backslash escapes,
//!   embedded newlines inside quoted fields);
//! - only substantive opinions are kept (`plain_text` >= `--min-chars`);
//! - `--cap` stops after N kept opinions (0 = no cap);
//! - `--prefix-gb` reads at most N GiB of COMPRESSED input (0 = all): the
//!   bulk file is id-ordered (roughly chronological, jurisdiction
//!   clumped), so a prefix sample is biased — fine for test corpora.
//!   A truncated tail ends the stream cleanly; the partial final row is
//!   dropped by the CSV reader's error.
//!
//! ```text
//! court_extract --input=opinions-2024-12-31.csv.bz2 \
//!     --output=opinions-sample.ndjson --cap=50000 --prefix-gb=4
//! ```

use std::fs::File;
use std::io::{BufRead, BufWriter, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use bzip2::read::MultiBzDecoder;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = arg("input", "/corpus/opinions.csv.bz2");
    let output = arg("output", "/corpus/opinions-sample.ndjson");
    let cap: u64 = arg("cap", "50000").parse()?;
    let min_chars: usize = arg("min-chars", "1000").parse()?;
    let prefix_gb: f64 = arg("prefix-gb", "0").parse()?;

    let file = File::open(&input)?;
    // --prefix-gb=0 means unlimited; only cap the stream when positive.
    let compressed: Box<dyn Read> = if prefix_gb > 0.0 {
        Box::new(file.take((prefix_gb * (1u64 << 30) as f64) as u64))
    } else {
        Box::new(file)
    };
    // Decode bz2 when the name says so; a truncated prefix ends the
    // decoder with an error, which the record loop treats as EOF.
    let reader: Box<dyn Read> = if input.ends_with(".bz2") {
        Box::new(MultiBzDecoder::new(compressed))
    } else {
        compressed
    };

    // The bulk export's quoting changed over time: snapshots through
    // 2024-12-31 quote with BACKTICKS (doubled backtick escapes), the
    // 2025-01-24+ exports quote with double quotes (backslash escapes).
    // Getting this wrong is catastrophic — an unrecognized quote means
    // embedded quotes in plain_text open phantom fields that swallow
    // megabytes. Detect from the first byte after the header line.
    let mut buffered = std::io::BufReader::new(reader);
    let quote = {
        let buf = buffered.fill_buf()?;
        let row1 = buf
            .iter()
            .position(|&b| b == b'\n')
            .and_then(|i| buf.get(i + 1));
        match row1 {
            Some(b'`') => b'`',
            _ => b'"',
        }
    };
    let mut builder = csv::ReaderBuilder::new();
    builder.quote(quote);
    if quote == b'"' {
        builder.escape(Some(b'\\'));
    }
    let mut csv = builder.from_reader(buffered);
    let header = csv.headers()?.clone();
    let ix = |name: &str| {
        header
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| format!("bulk CSV has no {name} column"))
    };
    let (ix_id, ix_cluster, ix_text) = (ix("id")?, ix("cluster_id")?, ix("plain_text")?);

    let mut out = BufWriter::new(File::create(PathBuf::from(&output))?);
    let (mut scanned, mut kept) = (0u64, 0u64);
    let t0 = Instant::now();
    let mut record = csv::StringRecord::new();
    loop {
        match csv.read_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {}
            // Truncated prefix or a partial final row: keep what we have.
            Err(_) => break,
        }
        scanned += 1;
        if scanned % 200_000 == 0 {
            eprintln!("extract: scanned {scanned}, kept {kept}");
        }
        let (Some(id), Some(cluster), Some(text)) = (
            record.get(ix_id),
            record.get(ix_cluster),
            record.get(ix_text),
        ) else {
            continue; // short row
        };
        if text.len() < min_chars {
            continue;
        }
        let line = serde_json::json!({
            "id": id,
            "cluster_id": cluster,
            "plain_text": text,
        });
        out.write_all(line.to_string().as_bytes())?;
        out.write_all(b"\n")?;
        kept += 1;
        if cap > 0 && kept >= cap {
            break;
        }
    }
    out.flush()?;
    eprintln!(
        "extract: scanned {scanned}, kept {kept} in {:?} -> {output}",
        t0.elapsed()
    );
    Ok(())
}
