//! Court pipeline stage 3: ingest the chunked, embedded corpus into an
//! N-shard pipestream-search cluster on loopback — calibration fit +
//! BroadcastVectorBackend, AddDocuments (chunk texts WITH lineage, real
//! analysis sidecar for BM25) + AddVectors with aligned ids, Flush for
//! persistence — then run sample hybrid cascade queries.
//!
//! Shard assignment: contiguous blocks of the chunks-file order (chunk_id
//! ranges), preserving per-opinion locality within a shard where the
//! file order allows.
//!
//! Columns (`--cluster-meta=<tsv>`, lines of `cluster_id\tYYYY-MM-DD\tcourt`):
//! a chunk whose cluster is listed carries the facet `court`, the integer
//! `year`, and the timestamp `decided` (the filing date at midnight UTC),
//! so filters, partitioned compaction, and a placement tree have a key.
//! Nodes must then run with `--facet-fields=court --integer-fields=year,decided`.
//!
//! ```text
//! court_ingest --shards=4 --out-dir=/work/court-corpus/shards
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::demo::court::{self, Chunk};
use pipestream_search::harness::{self, mock_analysis, start_sidecar};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, BroadcastVectorBackendRequest,
    DocLineage, DocumentField, FacetValue, FlushRequest, GetDocumentsRequest, HealthRequest,
    IntegerValue, TimestampValue,
};
use pipestream_search::security::ToolClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const SIDECAR_BIN: &str =
    "/work/worktrees/turbovec-workspace/grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis";

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn analysis_spec() -> AnalysisSpec {
    pipestream_search::analyzer::body_spec()
}

/// The case_name field's analysis (docs/multi-field.md): names stay
/// UNSTEMMED (a party called "Fishing" must not match queries for
/// "fish"), tokens as identity, offsets kept for highlighting.
fn case_name_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: 1,
        stemmer: 1,
        term_vector_mode: 1,
        term_vector_source: 1,
        char_filters: vec![],
    }
}

/// Optional cluster metadata for the case_name field: a TSV of
/// `<cluster_id>\t<case name>` (one line per cluster; the rebuild
/// runbook exports it from the CourtListener clusters table with one
/// `\copy`). Absent = body-only ingest, exactly as before.
fn load_case_names(path: &str) -> Result<std::collections::HashMap<u64, String>, String> {
    use std::io::BufRead;
    let file = std::fs::File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut map = std::collections::HashMap::new();
    for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|e| format!("{path}:{}: {e}", i + 1))?;
        if line.is_empty() {
            continue;
        }
        let Some((id, name)) = line.split_once('\t') else {
            return Err(format!("{path}:{}: expected <cluster_id>\\t<name>", i + 1));
        };
        let id: u64 = id
            .trim()
            .parse()
            .map_err(|e| format!("{path}:{}: cluster id: {e}", i + 1))?;
        let name = name.trim();
        if !name.is_empty() {
            map.insert(id, name.to_string());
        }
    }
    Ok(map)
}

/// One extra column over the SAME body text, under its own analysis:
/// the A/B unit.
///
/// Comparing two analysis chains normally costs two indexes and two
/// ingests, which at corpus scale is hours and hundreds of GB. Multi-field
/// BM25 already gives every field its own postings over one shared slot
/// space, so the same text indexed twice under different specs is two
/// columns of ONE index, and the comparison becomes a query-time choice of
/// which field to score. Both columns see byte-identical input and
/// identical ids, which is exactly the control a text-scrub or
/// re-chunk experiment cannot offer.
///
/// Cost is the honest catch: a body column is most of the postings, so
/// each one roughly adds the whole `.bm25` again. Slices, not corpora.
struct BodyColumn {
    field: String,
    spec: AnalysisSpec,
}

/// Parses `--body-columns=name:tokenizer:stemmer:source,...` (numeric
/// sidecar enum values, matching AnalysisSpec's passthrough), each an
/// extra copy of the body under that spec. The stored body itself is
/// always field 0 and is NOT named here.
fn parse_body_columns(spec: &str) -> Result<Vec<BodyColumn>, String> {
    let mut out = Vec::new();
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 4 {
            return Err(format!(
                "--body-columns entry {entry:?}: expected name:tokenizer:stemmer:source"
            ));
        }
        let num = |i: usize, what: &str| -> Result<i32, String> {
            parts[i]
                .parse()
                .map_err(|e| format!("--body-columns entry {entry:?}: {what}: {e}"))
        };
        let field = parts[0].to_string();
        if field == "body" {
            return Err("--body-columns cannot redefine \"body\": it is field 0, \
                        ingested under --analysis"
                .to_string());
        }
        out.push(BodyColumn {
            field,
            spec: AnalysisSpec {
                tokenizer: num(1, "tokenizer")?,
                stemmer: num(2, "stemmer")?,
                term_vector_mode: 1,
                term_vector_source: num(3, "source")?,
                char_filters: Vec::new(),
            },
        });
    }
    Ok(out)
}

/// The extra-field entries for one chunk: every A/B body column over the
/// chunk's own text, then a case_name DocumentField when the cluster map
/// knows the chunk's cluster.
/// One cluster's filing date and court from `--cluster-meta`.
#[derive(Clone, Debug)]
struct ClusterMeta {
    year: i64,
    /// The filing date at midnight UTC.
    decided: prost_types::Timestamp,
    court: String,
}

/// `cluster_id\tYYYY-MM-DD\tcourt` per line; an empty date or court
/// leaves that value absent for the cluster.
fn load_cluster_meta(path: &str) -> Result<HashMap<u64, ClusterMeta>, String> {
    use std::io::BufRead;
    let file = std::fs::File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut map = HashMap::new();
    for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|e| format!("{path}:{}: {e}", i + 1))?;
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split('\t');
        let id: u64 = parts
            .next()
            .unwrap_or("")
            .trim()
            .parse()
            .map_err(|e| format!("{path}:{}: cluster id: {e}", i + 1))?;
        let date = parts.next().unwrap_or("").trim();
        let court = parts.next().unwrap_or("").trim();
        if date.is_empty() || court.is_empty() {
            continue;
        }
        let (year, seconds) = parse_civil_date(date)
            .ok_or_else(|| format!("{path}:{}: date {date:?} is not YYYY-MM-DD", i + 1))?;
        map.insert(
            id,
            ClusterMeta {
                year,
                decided: prost_types::Timestamp { seconds, nanos: 0 },
                court: court.to_string(),
            },
        );
    }
    Ok(map)
}

/// `YYYY-MM-DD` to (year, seconds since the Unix epoch at midnight UTC),
/// by the proleptic Gregorian day count. No calendar crate: the input is
/// one fixed shape and the arithmetic is a dozen lines.
fn parse_civil_date(text: &str) -> Option<(i64, i64)> {
    let mut it = text.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if it.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's days_from_civil.
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some((y, days * 86_400))
}

/// The typed column values of one chunk from its cluster's metadata.
fn cluster_columns(
    meta: &Option<std::sync::Arc<HashMap<u64, ClusterMeta>>>,
    cluster_id: u64,
) -> (Vec<FacetValue>, Vec<IntegerValue>, Vec<TimestampValue>) {
    let Some(m) = meta.as_ref().and_then(|m| m.get(&cluster_id)) else {
        return (Vec::new(), Vec::new(), Vec::new());
    };
    (
        vec![FacetValue {
            field: "court".to_string(),
            value: m.court.clone(),
        }],
        vec![IntegerValue {
            field: "year".to_string(),
            value: m.year,
        }],
        vec![TimestampValue {
            field: "decided".to_string(),
            value: Some(m.decided.clone()),
        }],
    )
}

fn chunk_fields(
    case_names: &Option<std::sync::Arc<std::collections::HashMap<u64, String>>>,
    cluster_id: u64,
    body_columns: &[BodyColumn],
    text: &str,
) -> Vec<DocumentField> {
    let mut fields: Vec<DocumentField> = body_columns
        .iter()
        .map(|c| DocumentField {
            field: c.field.clone(),
            text: text.to_string(),
            analysis: Some(c.spec.clone()),
        })
        .collect();
    if let Some(name) = case_names.as_ref().and_then(|m| m.get(&cluster_id)) {
        fields.push(DocumentField {
            field: "case_name".to_string(),
            text: name.clone(),
            analysis: Some(case_name_spec()),
        });
    }
    fields
}

/// Sequential reader over an embeddings-file block starting at record
/// `start`, reached by a direct seek (records are fixed stride: 8-byte
/// opinion_id, 4-byte ordinal, dim little-endian f32s after the 12-byte
/// header).
struct EmbBlock {
    reader: std::io::BufReader<std::fs::File>,
    dim: usize,
}

impl EmbBlock {
    fn open(path: &str, start: u64, dim: usize) -> std::io::Result<Self> {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(12 + start * (12 + dim as u64 * 4)))?;
        Ok(Self {
            reader: std::io::BufReader::with_capacity(1 << 20, file),
            dim,
        })
    }

    /// The next record's key and vector together.
    fn next_record(&mut self) -> std::io::Result<((u64, u32), Vec<f32>)> {
        use std::io::Read;
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let key = (
            u64::from_le_bytes(fixed[..8].try_into().unwrap()),
            u32::from_le_bytes(fixed[8..12].try_into().unwrap()),
        );
        let mut buf = vec![0u8; self.dim * 4];
        self.reader.read_exact(&mut buf)?;
        Ok((
            key,
            buf.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect(),
        ))
    }

    /// The next record's vector.
    fn next_vector(&mut self) -> std::io::Result<Vec<f32>> {
        use std::io::Read;
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let mut buf = vec![0u8; self.dim * 4];
        self.reader.read_exact(&mut buf)?;
        Ok(buf
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| f32::from_le_bytes(*c))
            .collect())
    }
}

/// A fitted TQ+ seed calibration, shared between the drivers of one
/// rebuild (`--calibration=<path>`). Fitting streams the whole
/// embeddings file, so N parallel drivers would otherwise read it N
/// times to arrive at the same numbers.
#[derive(serde::Serialize, serde::Deserialize)]
struct CalibrationFile {
    dim: usize,
    bit_width: usize,
    shift: Vec<f32>,
    scale: Vec<f32>,
}

/// Load `path` when it exists, else fit from the embeddings file's
/// stride sample and write it (temp file + rename, so a concurrent
/// reader never sees a partial fit).
fn load_or_fit_calibration(
    path: &str,
    embeddings_path: &str,
    dim: usize,
) -> Result<(Vec<f32>, Vec<f32>), Box<dyn std::error::Error>> {
    if !path.is_empty() && std::path::Path::new(path).exists() {
        let text = std::fs::read_to_string(path)?;
        let file: CalibrationFile = serde_json::from_str(&text)?;
        if file.dim != dim {
            return Err(format!("{path}: calibration dim {} != corpus dim {dim}", file.dim).into());
        }
        eprintln!("calibration loaded from {path}");
        return Ok((file.shift, file.scale));
    }
    let (_, reader) = court::EmbeddingReader::open(std::path::Path::new(embeddings_path))?;
    let mut sample: Vec<f32> = Vec::new();
    for (i, record) in reader.enumerate() {
        let record = record?;
        if i % 300 == 0 {
            sample.extend_from_slice(&record.vector);
        }
    }
    let (shift, scale) = harness::fit_calibration(dim, 4, &sample);
    eprintln!(
        "calibration fitted on {} sample vectors",
        sample.len() / dim
    );
    if !path.is_empty() {
        let text = serde_json::to_string(&CalibrationFile {
            dim,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })?;
        let tmp = format!("{path}.tmp{}", std::process::id());
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, path)?;
        eprintln!("calibration written to {path}");
    }
    Ok((shift, scale))
}

/// Remote-shard mode: stream the chunks and embeddings files shard by
/// shard into already-running nodes (`--nodes`), instead of building the
/// join in memory. Nothing is buffered beyond the channel window, so the
/// driver stays in the tens of MB at any corpus size.
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The fleet's security flags (docs/security.md): --tls-ca and the
    // client identity the node listeners demand. Installed process-wide
    // so the in-process coordinator's channels carry the same identity.
    let security = ToolClient::from_env_args()?;
    security.install();
    let nodes_arg = arg("nodes", "");
    if !nodes_arg.is_empty() {
        return run_remote(nodes_arg, security).await;
    }

    let chunks_path = arg("chunks", "/work/court-corpus/chunks.ndjson");
    let embeddings_path = arg("embeddings", "/work/court-corpus/embeddings.bin");
    let out_dir = arg("out-dir", "/work/court-corpus/shards");
    let n_shards: usize = arg("shards", "4").parse()?;
    let limit: usize = arg("limit", "0").parse()?;
    let sidecar_port: u16 = arg("sidecar-port", "59101").parse()?;
    std::fs::create_dir_all(&out_dir)?;
    // Optional cluster_id -> case name map (--case-names=<tsv>): chunks
    // whose cluster is known carry a "case_name" DocumentField
    // (docs/multi-field.md).
    let case_names: Option<std::sync::Arc<std::collections::HashMap<u64, String>>> =
        match arg("case-names", "").as_str() {
            "" => None,
            path => Some(std::sync::Arc::new(load_case_names(path)?)),
        };
    let cluster_meta: Option<std::sync::Arc<HashMap<u64, ClusterMeta>>> =
        match arg("cluster-meta", "").as_str() {
            "" => None,
            path => Some(std::sync::Arc::new(load_cluster_meta(path)?)),
        };
    let body_columns = std::sync::Arc::new(parse_body_columns(&arg("body-columns", ""))?);

    // --- Load and join chunks x embeddings -------------------------------
    let t0 = Instant::now();
    let mut chunks: Vec<Chunk> = Vec::new();
    for chunk in court::read_chunks(std::path::Path::new(&chunks_path))? {
        chunks.push(chunk?);
        if limit > 0 && chunks.len() >= limit {
            break;
        }
    }
    let mut embeddings: HashMap<(u64, u32), Vec<f32>> = HashMap::new();
    let (dim, reader) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
    let dim = dim as usize;
    for record in reader {
        let record = record?;
        embeddings.insert((record.opinion_id, record.ordinal), record.vector);
    }
    let mut joined: Vec<(Chunk, Vec<f32>)> = Vec::with_capacity(chunks.len());
    let mut missing = 0usize;
    for chunk in chunks {
        match embeddings.remove(&(chunk.opinion_id, chunk.ordinal)) {
            Some(vector) => joined.push((chunk, vector)),
            None => missing += 1,
        }
    }
    eprintln!(
        "loaded {} chunks joined with dim-{dim} embeddings ({missing} without embeddings) in {:?}",
        joined.len(),
        t0.elapsed()
    );
    let m = joined.len();

    // --- Calibration on a stride sample ----------------------------------
    let sample: Vec<f32> = joined
        .iter()
        .step_by(37)
        .flat_map(|(_, v)| v.iter().copied())
        .collect();
    let (shift, scale) = harness::fit_calibration(dim, 4, &sample);
    eprintln!(
        "calibration fitted on {} sample vectors",
        sample.len() / dim
    );

    // --- Sidecar (real; mock only with --allow-mock) ----------------------
    // A mock-analyzed BM25 index looks healthy and scores garbage, so
    // falling back silently is never acceptable for a real corpus.
    let mut sidecar_child = None;
    let analysis_addr = match start_sidecar(&arg("sidecar-bin", SIDECAR_BIN), sidecar_port) {
        Ok((child, addr)) => {
            eprintln!("analysis sidecar: native binary at {addr}");
            sidecar_child = Some(child);
            addr
        }
        Err(e) if std::env::args().any(|a| a == "--allow-mock") => {
            eprintln!("WARNING: real sidecar unavailable ({e}); using the in-repo mock");
            mock_analysis::start_mock_analysis().await.0
        }
        Err(e) => return Err(format!("analysis sidecar unavailable: {e}").into()),
    };

    // --- Shard nodes + coordinator ----------------------------------------
    let per = m / n_shards;
    let mut node_addrs = Vec::new();
    let mut node_handles = Vec::new();
    for shard in 0..n_shards {
        let start = shard * per;
        let (addr, handle) = harness::start_empty_node(NodeConfig {
            slot_offset: start as u64,
            index_path: Some(PathBuf::from(&out_dir).join(format!("shard-{shard}.tv"))),
            analysis_addr: Some(analysis_addr.clone()),
            ..Default::default()
        })
        .await;
        node_addrs.push(addr);
        node_handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(node_addrs.clone())
        .with_bm25(Some(analysis_addr), Default::default());
    let backend = harness::embedded_backend_request(dim, 4, &shift, &scale);
    let results = coordinator
        .fanout_vector_backend(&BroadcastVectorBackendRequest {
            collection: String::new(),
            dim: backend.dim,
            config: backend.config,
        })
        .await;
    let resume = std::env::args().any(|a| a == "--resume");
    for r in &results {
        // A resumed shard's generation already carries its backend and
        // refuses a second configuration by name; that refusal is the
        // expected answer, not a failure.
        if !r.ok && resume && r.error.contains("locked for the generation") {
            eprintln!("vector backend already configured on {} (resuming)", r.node);
            continue;
        }
        assert!(r.ok, "vector backend rejected by {}: {}", r.node, r.error);
    }
    eprintln!("vector backend broadcast to {} shards OK", results.len());

    // --- Ingest: documents (with lineage) then vectors --------------------
    let spec = analysis_spec();
    let mut ingest_tasks = Vec::new();
    for (shard, addr) in node_addrs.iter().enumerate() {
        let start = shard * per;
        let end = if shard == n_shards - 1 {
            m
        } else {
            start + per
        };
        let block: Vec<(Chunk, Vec<f32>)> = joined[start..end].to_vec();
        let addr = addr.clone();
        let spec = spec.clone();
        let case_names = case_names.clone();
        let cluster_meta = cluster_meta.clone();
        let body_columns = body_columns.clone();
        let security = security.clone();
        ingest_tasks.push(tokio::spawn(async move {
            let n = block.len();
            // Documents first so doc ids and vector slots align 1:1.
            let mut client = NodeServiceClient::new(security.connect(&addr).await.unwrap());
            let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(64);
            let feeder = tokio::spawn(async move {
                for (i, (chunk, _)) in block.iter().enumerate() {
                    if i > 0 && i.is_multiple_of(20_000) {
                        eprintln!("  shard {shard}: {i}/{n} documents analyzed");
                    }
                    let (facets, integers, timestamps) =
                        cluster_columns(&cluster_meta, chunk.cluster_id);
                    tx.send(AddDocumentsRequest {
                        unsigned_integers: Vec::new(),
                        map_integers: Vec::new(),
                        map_unsigned_integers: Vec::new(),
                        original_source: None,
                        source_chunk_ordinal: None,
                        identity: None,
                        collection: String::new(),
                        cased_field: String::new(),
                        sentence_fields: Vec::new(),
                        materialize: None,
                        map_numerics: Vec::new(),
                        map_facets: Vec::new(),
                        numerics: Vec::new(),
                        facets,
                        text: chunk.text.clone(),
                        analysis: Some(spec.clone()),
                        lineage: Some(DocLineage {
                            parent_id: chunk.opinion_id,
                            group_id: chunk.cluster_id,
                            span_start: chunk.span_start,
                            span_end: chunk.span_end,
                        }),
                        fields: chunk_fields(
                            &case_names,
                            chunk.cluster_id,
                            &body_columns,
                            &chunk.text,
                        ),
                        integers,
                        timestamps,
                        geo_points: Vec::new(),
                        quality: None,
                        geography: None,
                        phrases: Vec::new(),
                        phrase_fingerprint: 0,
                        phrase_field: String::new(),
                        position_fields: Vec::new(),
                        bigram_fields: Vec::new(),
                    })
                    .await
                    .unwrap();
                }
                let vectors: Vec<f32> = block.into_iter().flat_map(|(_, v)| v).collect();
                (vectors, dim)
            });
            let docs = client
                .add_documents(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            let (vectors, dim) = feeder.await.unwrap();
            assert_eq!(docs.added as usize, n);

            let t0 = Instant::now();
            let (tx, rx) = mpsc::channel(8);
            let vf = tokio::spawn(async move {
                for batch in vectors.chunks(512 * dim) {
                    tx.send(AddVectorsRequest {
                        vectors: batch.to_vec(),
                        dim: dim as u32,
                    })
                    .await
                    .unwrap();
                }
            });
            let vecs = client
                .add_vectors(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            vf.await.unwrap();
            assert_eq!(vecs.added as usize, n);
            (shard, n, t0.elapsed())
        }));
    }
    for task in ingest_tasks {
        let (shard, n, t) = task.await?;
        eprintln!("shard {shard}: {n} chunks ingested (vectors in {t:?})");
    }

    // --- Flush -------------------------------------------------------------
    for (shard, addr) in node_addrs.iter().enumerate() {
        let mut client = NodeServiceClient::new(security.connect(addr).await.unwrap());
        let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
        assert!(flushed.written);
        eprintln!(
            "shard {shard}: flushed {} vectors + {} docs",
            flushed.num_vectors, flushed.num_documents
        );
    }

    // --- Sample hybrid cascade queries --------------------------------------
    let probes = [0usize, m / 2, m - 1];
    for probe in probes {
        let (chunk, vector) = &joined[probe];
        println!(
            "\n=== query (chunk {}, opinion {}, cluster {}): {:?}",
            chunk.chunk_id,
            chunk.opinion_id,
            chunk.cluster_id,
            &chunk.text[..chunk.text.len().min(120)]
        );
        let hits = coordinator
            .fanout_cascade(
                "court",
                &chunk.text,
                vector,
                5,
                Some(&spec),
                0.0,
                false,
                &Default::default(),
            )
            .await?
            .0;
        for hit in &hits {
            println!(
                "  #{} doc {:>7} (shard {}) vector {:.4}  bm25 {:.4}",
                hit.rank, hit.doc_id, hit.shard, hit.vector_score, hit.bm25_score
            );
        }
        if let Some(top) = hits.first() {
            let owner = node_addrs[top.shard as usize].clone();
            let mut client = NodeServiceClient::new(security.connect(&owner).await.unwrap());
            let docs = client
                .get_documents(GetDocumentsRequest {
                    doc_ids: vec![top.doc_id],
                })
                .await?
                .into_inner();
            if let Some(doc) = docs.documents.first() {
                let lineage = doc
                    .lineage
                    .as_ref()
                    .map(|l| {
                        format!(
                            "opinion {} cluster {} span {}..{}",
                            l.parent_id, l.group_id, l.span_start, l.span_end
                        )
                    })
                    .unwrap_or_default();
                println!("  top doc {}: {}", top.doc_id, lineage);
                println!("  top text: {:?}", &doc.text[..doc.text.len().min(160)]);
            }
        }
    }

    for handle in node_handles {
        handle.abort();
    }
    if let Some(mut child) = sidecar_child {
        let _ = child.kill();
    }
    eprintln!("\ningest complete; shards persisted under {out_dir}");
    Ok(())
}

/// Ingest into already-running shard nodes (e.g. on a second host).
/// Streams both files per shard instead of holding the full join.
async fn run_remote(
    nodes_arg: String,
    security: ToolClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunks_path = arg("chunks", "/work/court-corpus/chunks.ndjson");
    let embeddings_path = arg("embeddings", "/work/court-corpus/embeddings-static.bin");
    let node_addrs: Vec<String> = nodes_arg
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| security.url(s))
        .collect();
    let n_shards = node_addrs.len();
    if n_shards == 0 {
        return Err("--nodes must list at least one node".into());
    }
    // Optional shard range: run several drivers in parallel, each owning a
    // disjoint slice of the SAME full node list. Block offsets stay global
    // (per = m / n_shards), so the union of ranges reproduces the single
    // sequential run exactly.
    // Vectors-only ingest: skip the document phase and the analysis
    // sidecar entirely. Used to build vector-leg experiment clusters
    // (shard-count ladders) straight from the embeddings file.
    let vectors_only = std::env::args().any(|a| a == "--vectors-only");
    // `--resume`: continue a shard from the rows its node already holds
    // (a driver that died, a node restarted mid-ingest; the WAL keeps
    // the tail). The node reports documents and vectors; vectors the
    // last block did not finish are sent first, then the blocks resume
    // after the documents.
    let resume = std::env::args().any(|a| a == "--resume");
    // Optional cluster_id -> case name map (--case-names=<tsv>): chunks
    // whose cluster is known carry a "case_name" DocumentField, the
    // second real scoreable field (docs/multi-field.md). Nodes must run
    // with --bm25-fields=body,case_name.
    let case_names: Option<std::sync::Arc<std::collections::HashMap<u64, String>>> =
        match arg("case-names", "").as_str() {
            "" => None,
            path => Some(std::sync::Arc::new(load_case_names(path)?)),
        };
    let cluster_meta: Option<std::sync::Arc<HashMap<u64, ClusterMeta>>> =
        match arg("cluster-meta", "").as_str() {
            "" => None,
            path => Some(std::sync::Arc::new(load_cluster_meta(path)?)),
        };
    // Extra A/B columns over the same body text (--body-columns). Nodes
    // must carry these names in --bm25-fields, in this order, after body.
    let body_columns = std::sync::Arc::new(parse_body_columns(&arg("body-columns", ""))?);
    // Bandwidth-proportional fleets need unequal shards: explicit split
    // points (chunk indexes, ascending, exclusive ends; the last shard
    // runs to the corpus end) override the equal m/n block math.
    let split_points: Vec<usize> = match arg("split-points", "").as_str() {
        "" => Vec::new(),
        spec => spec
            .split(',')
            .map(|x| x.trim().parse())
            .collect::<Result<_, _>>()?,
    };
    let first_shard: usize = arg("first-shard", "0").parse()?;
    let end_shard: usize = match arg("end-shard", "").as_str() {
        "" => n_shards,
        s => s.parse()?,
    };
    if first_shard >= end_shard || end_shard > n_shards {
        return Err(format!(
            "--first-shard={first_shard} --end-shard={end_shard} out of range for {n_shards} nodes"
        )
        .into());
    }

    // Counts: positional ids == file order, established by the stage-1
    // integrity check (chunk count == embedding count). Vectors-only
    // ingest reads the count straight off the fixed-stride embeddings
    // file instead of walking the chunks file.
    // `--chunk-count=<m>` skips the counting walk, which reads the whole
    // chunks file (a rebuild's N parallel drivers would each pay it to
    // learn the same number). Correctness does not rest on the walk: the
    // doc feeder asserts (opinion_id, ordinal) equality at EVERY position
    // against the embeddings file, and each shard asserts it sent exactly
    // its share, so a wrong count cannot silently mis-join — it fails the
    // ingest. The plan stage prints the count to pass here.
    let declared_count: usize = arg("chunk-count", "0").parse()?;
    let m = if vectors_only || declared_count > 0 {
        let (dim_probe, _) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
        let rec = 12 + dim_probe as u64 * 4;
        let from_file = ((std::fs::metadata(&embeddings_path)?.len() - 12) / rec) as usize;
        if declared_count > 0 && declared_count != from_file {
            return Err(format!(
                "--chunk-count={declared_count} disagrees with the embeddings file's \
                 {from_file} records"
            )
            .into());
        }
        from_file
    } else {
        let mut m = 0usize;
        for chunk in court::read_chunks(std::path::Path::new(&chunks_path))? {
            chunk?;
            m += 1;
        }
        m
    };
    let per = m / n_shards;
    // Shard block bounds: equal blocks, or the explicit split points.
    let bounds: Vec<(usize, usize)> = if split_points.is_empty() {
        (0..n_shards)
            .map(|i| (i * per, if i == n_shards - 1 { m } else { (i + 1) * per }))
            .collect()
    } else {
        if split_points.len() != n_shards - 1 {
            return Err(format!(
                "--split-points needs {} points for {} nodes",
                n_shards - 1,
                n_shards
            )
            .into());
        }
        let mut edges = vec![0usize];
        edges.extend(&split_points);
        edges.push(m);
        if !edges.windows(2).all(|w| w[0] < w[1]) {
            return Err("--split-points must be ascending and inside the corpus".into());
        }
        edges.windows(2).map(|w| (w[0], w[1])).collect()
    };
    eprintln!(
        "remote ingest: {m} chunks over {n_shards} nodes ({per}/shard equal basis), \
         this driver handles shards {first_shard}..{end_shard}"
    );

    // Calibration: stride sample streamed from the embeddings file, or
    // the shared fit at --calibration=<path>. With per-block (v7)
    // calibration this is only the SEED every block starts from — a
    // sealed 8192-row block refits on its own rows — so it survives as
    // the open block's fit and as the precondition AddVectors checks.
    let (dim, _) = court::EmbeddingReader::open(std::path::Path::new(&embeddings_path))?;
    let dim = dim as usize;
    let calibration_path = arg("calibration", "");
    // Rows per ingest block (documents, then their vectors); see the
    // ingest loop below.
    let block_rows: usize = arg("ingest-block", "8192")
        .parse()
        .map_err(|e| format!("--ingest-block: {e}"))?;
    if block_rows == 0 {
        return Err("--ingest-block must be at least 1".into());
    }
    let (shift, scale) = load_or_fit_calibration(&calibration_path, &embeddings_path, dim)?;
    // `--fit-only` stops here: one driver fits and publishes the file,
    // then the per-shard drivers all load it instead of re-streaming the
    // embeddings file once each.
    if std::env::args().any(|a| a == "--fit-only") {
        eprintln!("--fit-only: calibration ready, no shards touched");
        return Ok(());
    }

    // Broadcast only to this driver's range: nodes outside it may already
    // hold vectors (another driver's shards), where provider configuration is a
    // failed_precondition rather than an idempotent retry.
    let coordinator = CoordinatorServiceImpl::new(node_addrs[first_shard..end_shard].to_vec())
        .with_bm25(
            Some(arg("analysis-addr", "http://127.0.0.1:59111")),
            Default::default(),
        );
    let backend = harness::embedded_backend_request(dim, 4, &shift, &scale);
    let results = coordinator
        .fanout_vector_backend(&BroadcastVectorBackendRequest {
            collection: String::new(),
            dim: backend.dim,
            config: backend.config,
        })
        .await;
    for r in &results {
        // A resumed shard's generation already carries its backend and
        // refuses a second configuration by name; that refusal is the
        // expected answer, not a failure.
        if !r.ok && resume && r.error.contains("locked for the generation") {
            eprintln!("vector backend already configured on {} (resuming)", r.node);
            continue;
        }
        assert!(r.ok, "vector backend rejected by {}: {}", r.node, r.error);
    }
    eprintln!("vector backend broadcast to {} shards OK", results.len());

    let spec = analysis_spec();
    for (shard, addr) in node_addrs
        .iter()
        .enumerate()
        .take(end_shard)
        .skip(first_shard)
    {
        let t0 = Instant::now();
        let (start, end) = bounds[shard];

        let n = end - start;
        let mut client = NodeServiceClient::new(security.connect(addr).await?);

        // Rows this node already holds, when resuming.
        let mut done = 0usize;
        if resume && !vectors_only {
            let health = client.health(HealthRequest {}).await?.into_inner();
            let docs = health.bm25_docs as usize;
            let vectors = health.num_vectors as usize;
            if docs > n || vectors > docs {
                return Err(format!(
                    "shard {shard}: node holds {docs} documents and {vectors} vectors; \
                     expected at most {n} documents and no more vectors than documents"
                )
                .into());
            }
            if vectors < docs {
                // The last block's documents landed, its vectors did not.
                let ep = embeddings_path.clone();
                let from = start + vectors;
                let count = docs - vectors;
                let vf = tokio::task::spawn_blocking(move || -> Result<Vec<f32>, String> {
                    let mut emb =
                        EmbBlock::open(&ep, from as u64, dim).map_err(|e| e.to_string())?;
                    let mut values = Vec::with_capacity(count * dim);
                    for _ in 0..count {
                        values.extend(emb.next_vector().map_err(|e| e.to_string())?);
                    }
                    Ok(values)
                });
                let values = vf.await??;
                let batches: Vec<AddVectorsRequest> = values
                    .chunks(512 * dim)
                    .map(|batch| AddVectorsRequest {
                        vectors: batch.to_vec(),
                        dim: dim as u32,
                    })
                    .collect();
                let response = client
                    .add_vectors(tokio_stream::iter(batches))
                    .await?
                    .into_inner();
                if response.added as usize != count {
                    return Err(format!(
                        "shard {shard}: resume sent {count} vectors, node added {}",
                        response.added
                    )
                    .into());
                }
                eprintln!("  shard {shard}: resume completed {count} vectors for rows {from}..");
            }
            done = docs;
            eprintln!("  shard {shard}: resuming after {done}/{n} rows");
        }

        if vectors_only {
            // Vectors for rows the shard already holds as documents (an
            // image rebuild over kept postings): a direct seek into the
            // fixed-stride embeddings file.
            let (tx, rx) = mpsc::channel::<AddVectorsRequest>(8);
            let ep = embeddings_path.clone();
            let vf = tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut emb = EmbBlock::open(&ep, start as u64, dim).map_err(|e| e.to_string())?;
                let mut batch: Vec<f32> = Vec::with_capacity(512 * dim);
                for _ in 0..n {
                    batch.extend(emb.next_vector().map_err(|e| e.to_string())?);
                    if batch.len() == 512 * dim {
                        tx.blocking_send(AddVectorsRequest {
                            vectors: std::mem::replace(&mut batch, Vec::with_capacity(512 * dim)),
                            dim: dim as u32,
                        })
                        .map_err(|e| e.to_string())?;
                    }
                }
                if !batch.is_empty() {
                    tx.blocking_send(AddVectorsRequest {
                        vectors: batch,
                        dim: dim as u32,
                    })
                    .map_err(|e| e.to_string())?;
                }
                Ok(())
            });
            let vecs = client
                .add_vectors(ReceiverStream::new(rx))
                .await?
                .into_inner();
            vf.await??;
            assert_eq!(vecs.added as usize, n);
        } else {
            // Documents and vectors in blocks: each block's documents,
            // then the same rows' vectors, before the next block. Ids
            // and slots align 1:1 within a block, and on the segment
            // layout a tail that holds both seals only when the two
            // agree, so the block is the longest a seal ever waits. The
            // reader walks the chunks file and the embeddings block in
            // lock step, asserting key equality at every position; both
            // files were written in the same order, so that equality IS
            // the join.
            let (btx, mut brx) = mpsc::channel::<(Vec<AddDocumentsRequest>, Vec<f32>)>(2);
            let spec2 = spec.clone();
            let case_names2 = case_names.clone();
            let cluster_meta2 = cluster_meta.clone();
            let body_columns2 = body_columns.clone();
            let cp = chunks_path.clone();
            let ep = embeddings_path.clone();
            let first_row = start + done;
            let feeder = tokio::task::spawn_blocking(move || -> Result<(), String> {
                use std::io::BufRead;
                let mut emb =
                    EmbBlock::open(&ep, first_row as u64, dim).map_err(|e| e.to_string())?;
                let file = std::fs::File::open(&cp).map_err(|e| e.to_string())?;
                let mut sent = 0usize;
                let mut docs: Vec<AddDocumentsRequest> = Vec::with_capacity(block_rows);
                let mut vectors: Vec<f32> = Vec::with_capacity(block_rows * dim);
                for (i, line) in std::io::BufReader::new(file).lines().enumerate() {
                    if i < first_row {
                        line.map_err(|e| e.to_string())?;
                        continue;
                    }
                    if i >= end {
                        break;
                    }
                    let line = line.map_err(|e| e.to_string())?;
                    let chunk: Chunk = serde_json::from_str(&line).map_err(|e| e.to_string())?;
                    let (key, vector) = emb.next_record().map_err(|e| e.to_string())?;
                    if key != (chunk.opinion_id, chunk.ordinal) {
                        return Err(format!(
                            "chunk/embedding order mismatch at shard {shard} position {i}: \
                         chunk ({}, {}), embedding ({}, {})",
                            chunk.opinion_id, chunk.ordinal, key.0, key.1
                        ));
                    }
                    // The extra columns index the same bytes, so build
                    // them before the body text moves into the request.
                    let fields =
                        chunk_fields(&case_names2, chunk.cluster_id, &body_columns2, &chunk.text);
                    let (facets, integers, timestamps) =
                        cluster_columns(&cluster_meta2, chunk.cluster_id);
                    docs.push(AddDocumentsRequest {
                        unsigned_integers: Vec::new(),
                        map_integers: Vec::new(),
                        map_unsigned_integers: Vec::new(),
                        original_source: None,
                        source_chunk_ordinal: None,
                        identity: None,
                        collection: String::new(),
                        cased_field: String::new(),
                        sentence_fields: Vec::new(),
                        materialize: None,
                        map_numerics: Vec::new(),
                        map_facets: Vec::new(),
                        numerics: Vec::new(),
                        facets,
                        text: chunk.text,
                        analysis: Some(spec2.clone()),
                        lineage: Some(DocLineage {
                            parent_id: chunk.opinion_id,
                            group_id: chunk.cluster_id,
                            span_start: chunk.span_start,
                            span_end: chunk.span_end,
                        }),
                        fields,
                        integers,
                        timestamps,
                        geo_points: Vec::new(),
                        quality: None,
                        geography: None,
                        phrases: Vec::new(),
                        phrase_fingerprint: 0,
                        phrase_field: String::new(),
                        position_fields: Vec::new(),
                        bigram_fields: Vec::new(),
                    });
                    vectors.extend(vector);
                    sent += 1;
                    if docs.len() == block_rows {
                        btx.blocking_send((
                            std::mem::replace(&mut docs, Vec::with_capacity(block_rows)),
                            std::mem::replace(&mut vectors, Vec::with_capacity(block_rows * dim)),
                        ))
                        .map_err(|e| e.to_string())?;
                    }
                }
                if !docs.is_empty() {
                    btx.blocking_send((docs, vectors))
                        .map_err(|e| e.to_string())?;
                }
                if sent != n - done {
                    return Err(format!("shard {shard}: sent {sent} of {} rows", n - done));
                }
                Ok(())
            });
            let mut added_docs = done;
            let mut added_vectors = done;
            let mut next_report = (done / 100_000 + 1) * 100_000;
            while let Some((docs, vectors)) = brx.recv().await {
                let rows = docs.len();
                let response = client
                    .add_documents(tokio_stream::iter(docs))
                    .await?
                    .into_inner();
                if response.added as usize != rows {
                    return Err(format!(
                        "shard {shard}: a block of {rows} documents added {}",
                        response.added
                    )
                    .into());
                }
                added_docs += rows;
                let batches: Vec<AddVectorsRequest> = vectors
                    .chunks(512 * dim)
                    .map(|batch| AddVectorsRequest {
                        vectors: batch.to_vec(),
                        dim: dim as u32,
                    })
                    .collect();
                let response = client
                    .add_vectors(tokio_stream::iter(batches))
                    .await?
                    .into_inner();
                if response.added as usize != rows {
                    return Err(format!(
                        "shard {shard}: a block of {rows} vectors added {}",
                        response.added
                    )
                    .into());
                }
                added_vectors += rows;
                if added_docs >= next_report {
                    eprintln!("  shard {shard}: {added_docs}/{n} rows ingested");
                    next_report += 100_000;
                }
            }
            feeder.await??;
            assert_eq!(added_docs, n);
            assert_eq!(added_vectors, n);
        }

        let flushed = client.flush(FlushRequest {}).await?.into_inner();
        assert!(flushed.written);
        eprintln!(
            "shard {shard}: {n} chunks ingested + flushed ({} vectors, {} docs) in {:?}",
            flushed.num_vectors,
            flushed.num_documents,
            t0.elapsed()
        );
    }
    eprintln!("remote ingest complete");
    Ok(())
}
