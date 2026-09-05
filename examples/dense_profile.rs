//! Measure a dense quality profile against a live coordinator
//! (`docs/dense-quality-profile.md`) and write the version 2 file the
//! coordinator installs with `--dense-quality-profile`.
//!
//! The measuring logic is `pipestream_search::quality::measure`; this is
//! the CLI shell: it reads the held-out queries (a raw little-endian f32
//! rows file with `--dim`, or the court embeddings `.bin` record format
//! `examples/court_embed.rs` writes, detected by its header), samples
//! them, runs the ladder through the public `Query` route, prints the
//! table, and saves the profile.
//!
//! ```text
//! dense_profile --coord=http://127.0.0.1:59291 \
//!     --queries=/corpus/held-out.f32 --dim=256 --sample=128 --seed=7 \
//!     --k=10,100 --depths=10,20,50,100,200,500,1000,2000 \
//!     --targets=950000,990000,999000,1000000 \
//!     --ground-truth=brute:/corpus/embeddings.bin \
//!     --embedding-model=minilm-static-256 --profile-id=court-2026-09 \
//!     --default-target=990000 --out=/etc/protomolt/dense-quality.toml
//! ```
//!
//! `--ground-truth=brute-raw:<rows file>` maps a raw little-endian f32
//! rows file instead of reading it, for a corpus larger than memory;
//! `--export-raw=<out> --queries=<court .bin>` writes one from the court
//! format and exits.
//!
//! `--ground-truth=full-depth` (the default) takes the exhaustive FP32
//! order from the route itself at `selection_k = corpus rows`, which the
//! coordinator refuses above its `--max-k`; `brute:<rows file>` computes
//! it over a rows file whose record `i` is global doc id `i` and whose row
//! count must equal the corpus exactly.

use std::io::Read;
use std::path::Path;

use pipestream_search::demo::court;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::quality::measure::{
    describe_unmet, ladder_table, measure, GroundTruth, MeasureSpec,
};
use pipestream_search::security::ToolClient;
use pipestream_search::MAX_MESSAGE_BYTES;

type Error = Box<dyn std::error::Error + Send + Sync>;

fn arg(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

fn required(key: &str) -> Result<String, Error> {
    arg(key).ok_or_else(|| format!("--{key}=<value> is required").into())
}

fn parse_list(key: &str, default: Option<&str>) -> Result<Vec<u32>, Error> {
    let text = match (arg(key), default) {
        (Some(text), _) => text,
        (None, Some(default)) => default.to_string(),
        (None, None) => return Err(format!("--{key}=<comma list> is required").into()),
    };
    text.split(',')
        .map(|part| {
            part.trim()
                .parse::<u32>()
                .map_err(|error| format!("--{key}: invalid value {part:?}: {error}").into())
        })
        .collect()
}

/// Read a vectors file as `(dimensions, rows)`. The court embeddings
/// format announces itself with its magic; anything else is raw
/// little-endian f32 rows and needs `dim`.
fn read_rows(path: &Path, dim: Option<u32>) -> Result<(u32, Vec<f32>), Error> {
    let mut head = [0u8; 8];
    let magic = std::fs::File::open(path)?.read_exact(&mut head).is_ok()
        && &head == court::EMBEDDINGS_MAGIC;
    if magic {
        let (file_dim, reader) = court::EmbeddingReader::open(path)?;
        if let Some(dim) = dim {
            if dim != file_dim {
                return Err(format!(
                    "{}: header declares dimensions={file_dim}, --dim={dim} disagrees",
                    path.display()
                )
                .into());
            }
        }
        let mut rows = Vec::new();
        for record in reader {
            rows.extend_from_slice(&record?.vector);
        }
        return Ok((file_dim, rows));
    }
    let dim = dim.ok_or_else(|| {
        format!(
            "{} is a raw f32 rows file; --dim=<dimensions> is required",
            path.display()
        )
    })?;
    if dim == 0 {
        return Err("--dim must be positive".into());
    }
    let bytes = std::fs::read(path)?;
    let stride = dim as usize * 4;
    if bytes.is_empty() || !bytes.len().is_multiple_of(stride) {
        return Err(format!(
            "{}: {} bytes is not a positive multiple of {stride} (dimensions={dim} x 4)",
            path.display(),
            bytes.len()
        )
        .into());
    }
    let rows = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect();
    Ok((dim, rows))
}

/// Raw rows are little-endian; mapping them as f32 needs the host to be.
const _: () = assert!(cfg!(target_endian = "little"));

/// The brute-force rows: read into memory, or a raw rows file mapped.
enum BruteRows {
    Owned(Vec<f32>),
    Mapped(memmap2::Mmap),
}

impl BruteRows {
    fn rows(&self) -> &[f32] {
        match self {
            BruteRows::Owned(rows) => rows,
            BruteRows::Mapped(map) => {
                // A mapping is page-aligned and its length was checked to
                // be a multiple of the row stride, so the bytes are whole
                // f32 values on a little-endian host (pinned below).
                let (head, floats, tail) = unsafe { map.align_to::<f32>() };
                assert!(
                    head.is_empty() && tail.is_empty(),
                    "mapping is not f32-aligned"
                );
                floats
            }
        }
    }
}

/// Stream a court embeddings file's vectors into a raw little-endian f32
/// rows file (`--export-raw`), the input `brute-raw:` maps.
fn export_raw(from: &Path, to: &Path) -> Result<u64, Error> {
    use std::io::Write;
    let (dim, reader) = court::EmbeddingReader::open(from)?;
    let mut out = std::io::BufWriter::with_capacity(8 << 20, std::fs::File::create(to)?);
    let mut rows = 0u64;
    for record in reader {
        let record = record?;
        if record.vector.len() != dim as usize {
            return Err(format!(
                "row {rows}: {} floats, header says {dim}",
                record.vector.len()
            )
            .into());
        }
        for value in &record.vector {
            out.write_all(&value.to_le_bytes())?;
        }
        rows += 1;
    }
    out.flush()?;
    Ok(rows)
}

/// A seeded partial Fisher-Yates over the row indexes: the first `sample`
/// positions after shuffling, in shuffled order. splitmix64 keeps the
/// choice reproducible from `seed` alone.
fn sample_rows(rows: &[f32], dim: usize, sample: usize, seed: u64) -> Vec<f32> {
    let n = rows.len() / dim;
    if sample == 0 || sample >= n {
        return rows.to_vec();
    }
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut order: Vec<usize> = (0..n).collect();
    for i in 0..sample {
        let j = i + (next() % (n - i) as u64) as usize;
        order.swap(i, j);
    }
    let mut out = Vec::with_capacity(sample * dim);
    for &index in &order[..sample] {
        out.extend_from_slice(&rows[index * dim..(index + 1) * dim]);
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    if let Some(to) = arg("export-raw") {
        let from = required("queries")?;
        let rows = export_raw(Path::new(&from), Path::new(&to))?;
        println!("{rows} rows from {from} written as raw f32 rows to {to}");
        return Ok(());
    }
    let coord = required("coord")?;
    let collection = arg("collection").unwrap_or_default();
    let queries_path = required("queries")?;
    let dim_hint = arg("dim").map(|d| d.parse::<u32>()).transpose()?;
    let sample: usize = arg("sample").map(|s| s.parse()).transpose()?.unwrap_or(0);
    let seed: u64 = arg("seed")
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(0x5EED);
    let ks = parse_list("k", None)?;
    let depths = parse_list("depths", None)?;
    let targets = parse_list("targets", Some("950000,990000,999000,1000000"))?;
    let embedding_model = required("embedding-model")?;
    let profile_id = required("profile-id")?;
    let default_target = arg("default-target")
        .map(|t| t.parse::<u32>())
        .transpose()?;
    let out = required("out")?;
    let ground_truth_arg = arg("ground-truth").unwrap_or_else(|| "full-depth".to_string());

    let (dim, all_queries) = read_rows(Path::new(&queries_path), dim_hint)?;
    let queries = sample_rows(&all_queries, dim as usize, sample, seed);
    eprintln!(
        "{} held-out queries of dimensions={dim} ({} in {queries_path}, seed {seed})",
        queries.len() / dim as usize,
        all_queries.len() / dim as usize
    );
    let brute_rows = match ground_truth_arg.as_str() {
        "full-depth" => None,
        other => match (
            other.strip_prefix("brute:"),
            other.strip_prefix("brute-raw:"),
        ) {
            (Some(path), _) => {
                let (rows_dim, rows) = read_rows(Path::new(path), dim_hint)?;
                if rows_dim != dim {
                    return Err(format!(
                        "brute rows have dimensions={rows_dim}, queries have {dim}"
                    )
                    .into());
                }
                eprintln!(
                    "brute ground truth over {} rows from {path}",
                    rows.len() / dim as usize
                );
                Some(BruteRows::Owned(rows))
            }
            (_, Some(path)) => {
                // A raw little-endian f32 rows file, mapped rather than
                // read: the full court corpus is 89 GB of rows, more than
                // this box holds twice over, and the page cache serves
                // the one pass the ground truth makes.
                let file = std::fs::File::open(path)?;
                let map = unsafe { memmap2::Mmap::map(&file)? };
                let stride = dim as usize * 4;
                if map.is_empty() || !map.len().is_multiple_of(stride) {
                    return Err(format!(
                        "{path}: {} bytes is not a positive multiple of {stride} (dimensions={dim} x 4)",
                        map.len()
                    )
                    .into());
                }
                eprintln!(
                    "brute ground truth over {} mapped rows from {path}",
                    map.len() / stride
                );
                Some(BruteRows::Mapped(map))
            }
            (None, None) => {
                return Err(format!(
                    "--ground-truth={other:?}: expected full-depth, brute:<rows file>, or \
                     brute-raw:<raw f32 rows file>"
                )
                .into())
            }
        },
    };
    let ground_truth = match &brute_rows {
        Some(rows) => GroundTruth::Brute { rows: rows.rows() },
        None => GroundTruth::FullDepth,
    };

    // The fleet's id layout, when its shards sit at a slot stride wider
    // than their rows (the runbook's OFFSET_STRIDE): row positions of
    // the brute file map to `shard * stride + local`.
    let brute_id_map = match (arg("brute-rows-per-shard"), arg("brute-slot-stride")) {
        (None, None) => None,
        (Some(rows), Some(stride)) => Some(pipestream_search::quality::measure::BruteIdMap {
            rows_per_shard: rows
                .parse()
                .map_err(|e| format!("--brute-rows-per-shard: {e}"))?,
            slot_stride: stride
                .parse()
                .map_err(|e| format!("--brute-slot-stride: {e}"))?,
        }),
        _ => return Err("--brute-rows-per-shard and --brute-slot-stride go together".into()),
    };
    let security = ToolClient::from_env_args()?;
    let mut client =
        SearchServiceClient::with_interceptor(security.connect(&coord).await?, security.bearer())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let spec = MeasureSpec {
        collection,
        profile_id,
        embedding_model,
        queries: &queries,
        dimensions: dim,
        ks,
        depths,
        targets,
        default_target_recall_ppm: default_target,
        ground_truth,
        brute_id_map,
    };
    let measured = measure(&mut client, &spec).await?;
    eprintln!(
        "identity: provider {} fingerprint {} dimensions {} rows {} generation {}",
        measured.identity.provider_backend,
        measured.identity.scoring_fingerprint,
        measured.identity.dimensions,
        measured.identity.rows,
        measured.identity.topology_generation
    );
    print!("{}", ladder_table(&measured));
    if !measured.unmet.is_empty() {
        eprintln!("unmet targets: {}", describe_unmet(&measured.unmet));
    }
    for point in measured.profile.points() {
        println!(
            "point k={} target_recall_ppm={} candidates={}",
            point.k, point.target_recall_ppm, point.candidates
        );
    }
    measured.profile.save(Path::new(&out))?;
    println!(
        "wrote {out} (fingerprint {})",
        measured.profile.fingerprint()
    );
    Ok(())
}
