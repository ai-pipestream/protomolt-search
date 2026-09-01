//! Neutral, deterministic challenge driver for Protomolt Search and
//! OpenSearch. The operational contract lives in deploy/opensearch-challenge.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request as HttpRequest, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::rt::TokioExecutor;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    query_stream_response, search_query, selection_query, selection_score_strategy,
    AddDocumentsRequest, AddVectorsRequest, ClusterHealthRequest, DenseQuery, DocLineage,
    FacetValue, FilterQuery, HealthRequest, IntegerValue, LexicalQuery, QueryRequest,
    QueryStreamPhase, QueryStreamRequest, RrfScore, SearchQuery, SearchRequest, SelectionOperator,
    SelectionQuery, SelectionScoreStrategy, SetCalibrationRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Document {
    id: u64,
    text: String,
    topic: u32,
    year: i64,
    group: String,
    parent: u64,
    vector: Vec<f32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Judgment {
    doc_id: u64,
    gain: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkloadKind {
    Lexical,
    Vector,
    Hybrid,
    FilteredLexical,
    FilteredVector,
    CollapseVector,
}

impl WorkloadKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Lexical => "lexical",
            Self::Vector => "vector",
            Self::Hybrid => "hybrid",
            Self::FilteredLexical => "filtered_lexical",
            Self::FilteredVector => "filtered_vector",
            Self::CollapseVector => "collapse_vector",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Workload {
    id: String,
    kind: WorkloadKind,
    text: String,
    vector: Vec<f32>,
    k: u32,
    min_year: Option<i64>,
    judgments: Vec<Judgment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    format: String,
    seed: u64,
    documents: usize,
    dimensions: usize,
    topics: usize,
    workloads: usize,
    corpus_sha256: String,
    workload_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Sample {
    record_type: String,
    engine: String,
    query_id: String,
    workload: String,
    iteration: usize,
    concurrency: usize,
    ok: bool,
    completed: bool,
    ttfh_ms: f64,
    latency_ms: f64,
    hit_ids: Vec<u64>,
    error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Cell {
    record_type: String,
    engine: String,
    concurrency: usize,
    requests: usize,
    completed: usize,
    elapsed_ms: f64,
    qps: f64,
}

fn option(key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    std::env::args().find_map(|arg| arg.strip_prefix(&prefix).map(str::to_string))
}

fn required(key: &str) -> Result<String, Error> {
    option(key).ok_or_else(|| format!("--{key}=... is required").into())
}

fn parsed<T>(key: &str, default: T) -> Result<T, Error>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    option(key).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("--{key}={value:?}: {error}").into())
    })
}

fn normalize_addr(value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        value.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", value.trim_end_matches('/'))
    }
}

fn write_json_lines<T: Serialize>(path: &Path, rows: &[T]) -> Result<(), Error> {
    let mut out = BufWriter::new(File::create(path)?);
    for row in rows {
        serde_json::to_writer(&mut out, row)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(())
}

fn read_json_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, Error> {
    BufReader::new(File::open(path)?)
        .lines()
        .enumerate()
        .map(|(line, value)| {
            let value =
                value.map_err(|error| format!("{}:{}: {error}", path.display(), line + 1))?;
            serde_json::from_str(&value)
                .map_err(|error| format!("{}:{}: {error}", path.display(), line + 1).into())
        })
        .collect()
}

fn file_digest(path: &Path) -> Result<String, Error> {
    Ok(pipestream_search::sha256::hex_digest(&std::fs::read(path)?))
}

#[derive(Clone, Copy)]
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let unit = ((self.0 >> 40) as u32) as f32 / ((1u32 << 24) - 1) as f32;
        unit * 2.0 - 1.0
    }
}

fn normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt() as f32;
    for value in vector {
        *value /= norm;
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn exact_vector_judgments(
    docs: &[Document],
    query: &[f32],
    min_year: Option<i64>,
    collapse: bool,
    depth: usize,
) -> Vec<Judgment> {
    let mut rows: Vec<(u64, u64, f32)> = docs
        .iter()
        .filter(|doc| min_year.is_none_or(|year| doc.year >= year))
        .map(|doc| (doc.id, doc.parent, dot(&doc.vector, query)))
        .collect();
    rows.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    if collapse {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|(_, parent, _)| seen.insert(*parent));
    }
    rows.into_iter()
        .take(depth)
        .enumerate()
        .map(|(rank, (doc_id, _, _))| Judgment {
            doc_id,
            gain: u32::try_from(depth - rank).unwrap_or(u32::MAX),
        })
        .collect()
}

fn lexical_judgments(
    docs: &[Document],
    topic: u32,
    min_year: Option<i64>,
    depth: usize,
) -> Vec<Judgment> {
    docs.iter()
        .filter(|doc| doc.topic == topic && min_year.is_none_or(|year| doc.year >= year))
        .take(depth)
        .map(|doc| Judgment {
            doc_id: doc.id,
            gain: 1,
        })
        .collect()
}

fn generate() -> Result<(), Error> {
    let out = PathBuf::from(required("out")?);
    std::fs::create_dir_all(&out)?;
    let count: usize = parsed("documents", 4096)?;
    let dim: usize = parsed("dimensions", 64)?;
    let topics: usize = parsed("topics", 16)?;
    let seed: u64 = parsed("seed", 0xC0DE_51A7_u64)?;
    let k: u32 = parsed("k", 10)?;
    if count == 0 || dim < topics || topics == 0 {
        return Err("documents > 0 and dimensions >= topics > 0 are required".into());
    }
    let mut rng = Lcg(seed);
    let mut docs = Vec::with_capacity(count);
    for id in 0..count {
        let topic = id % topics;
        let mut vector: Vec<f32> = (0..dim).map(|_| rng.next() * 0.075).collect();
        vector[topic] += 1.0;
        vector[(topic * 7 + 19) % dim] += 0.25;
        normalize(&mut vector);
        docs.push(Document {
            id: id as u64,
            text: format!(
                "common retrieval topic{topic:02} topic{topic:02} shardless token{:03}",
                id % 251
            ),
            topic: topic as u32,
            year: 1980 + (id % 45) as i64,
            group: format!("topic{topic:02}"),
            parent: (id / 3 + 1) as u64,
            vector,
        });
    }

    let mut workloads = Vec::new();
    for topic in 0..topics {
        let mut query: Vec<f32> = (0..dim).map(|_| rng.next() * 0.015).collect();
        query[topic] += 1.0;
        query[(topic * 7 + 19) % dim] += 0.25;
        normalize(&mut query);
        let text = format!("topic{topic:02}");
        let min_year = 2005;
        let vector_qrels = exact_vector_judgments(&docs, &query, None, false, k as usize);
        let lexical_qrels = lexical_judgments(&docs, topic as u32, None, docs.len());
        let filtered_vector_qrels =
            exact_vector_judgments(&docs, &query, Some(min_year), false, k as usize);
        let filtered_lexical_qrels =
            lexical_judgments(&docs, topic as u32, Some(min_year), docs.len());
        let collapse_qrels = exact_vector_judgments(&docs, &query, None, true, k as usize);
        let mut hybrid_gain: HashMap<u64, u32> = lexical_qrels
            .iter()
            .map(|judgment| (judgment.doc_id, 2))
            .collect();
        for judgment in &vector_qrels {
            *hybrid_gain.entry(judgment.doc_id).or_insert(0) += 1;
        }
        let mut hybrid_qrels: Vec<Judgment> = hybrid_gain
            .into_iter()
            .map(|(doc_id, gain)| Judgment { doc_id, gain })
            .collect();
        hybrid_qrels.sort_by(|a, b| b.gain.cmp(&a.gain).then_with(|| a.doc_id.cmp(&b.doc_id)));

        let cases = [
            (WorkloadKind::Lexical, None, lexical_qrels),
            (WorkloadKind::Vector, None, vector_qrels.clone()),
            (WorkloadKind::Hybrid, None, hybrid_qrels),
            (
                WorkloadKind::FilteredLexical,
                Some(min_year),
                filtered_lexical_qrels,
            ),
            (
                WorkloadKind::FilteredVector,
                Some(min_year),
                filtered_vector_qrels,
            ),
            (WorkloadKind::CollapseVector, None, collapse_qrels),
        ];
        for (kind, year, judgments) in cases {
            workloads.push(Workload {
                id: format!("{}-{topic:02}", kind.as_str()),
                kind,
                text: text.clone(),
                vector: query.clone(),
                k,
                min_year: year,
                judgments,
            });
        }
    }
    let corpus_path = out.join("corpus.jsonl");
    let workload_path = out.join("workload.jsonl");
    write_json_lines(&corpus_path, &docs)?;
    write_json_lines(&workload_path, &workloads)?;
    let manifest = Manifest {
        format: "protomolt-opensearch-challenge-v1".into(),
        seed,
        documents: docs.len(),
        dimensions: dim,
        topics,
        workloads: workloads.len(),
        corpus_sha256: file_digest(&corpus_path)?,
        workload_sha256: file_digest(&workload_path)?,
    };
    std::fs::write(
        out.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!("{}", serde_json::to_string(&manifest)?);
    Ok(())
}

async fn serve_mock() -> Result<(), Error> {
    let (address, handle) = pipestream_search::harness::mock_analysis::start_mock_analysis().await;
    println!("{}", json!({"ready": true, "address": address}));
    std::io::stdout().flush()?;
    tokio::select! {
        result = handle => { result??; }
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}

async fn ingest_protomolt() -> Result<(), Error> {
    let node = normalize_addr(&required("node")?);
    let corpus = read_json_lines::<Document>(Path::new(&required("corpus")?))?;
    let dim = corpus.first().ok_or("empty corpus")?.vector.len();
    if corpus.iter().any(|doc| doc.vector.len() != dim) {
        return Err("corpus vector dimensions differ".into());
    }
    let started = Instant::now();
    let vectors: Vec<f32> = corpus
        .iter()
        .flat_map(|doc| doc.vector.iter().copied())
        .collect();
    let (shift, scale) = pipestream_search::harness::fit_calibration(dim, 4, &vectors);
    let mut client = NodeServiceClient::connect(node).await?;
    client
        .set_calibration(SetCalibrationRequest {
            dim: dim as u32,
            bit_width: 4,
            shift,
            scale,
        })
        .await?;

    let (doc_tx, doc_rx) = mpsc::channel(32);
    let docs_for_send = corpus.clone();
    let sender = tokio::spawn(async move {
        for doc in docs_for_send {
            doc_tx
                .send(AddDocumentsRequest {
                    text: doc.text,
                    lineage: Some(DocLineage {
                        parent_id: doc.parent,
                        group_id: u64::from(doc.topic),
                        ..Default::default()
                    }),
                    facets: vec![FacetValue {
                        field: "group".into(),
                        value: doc.group,
                    }],
                    integers: vec![IntegerValue {
                        field: "year".into(),
                        value: doc.year,
                    }],
                    ..Default::default()
                })
                .await?;
        }
        Ok::<_, Error>(())
    });
    let doc_response = client
        .add_documents(ReceiverStream::new(doc_rx))
        .await?
        .into_inner();
    sender.await??;

    let (vec_tx, vec_rx) = mpsc::channel(4);
    let vector_sender = tokio::spawn(async move {
        for batch in vectors.chunks(dim * 256) {
            vec_tx
                .send(AddVectorsRequest {
                    vectors: batch.to_vec(),
                    dim: dim as u32,
                })
                .await?;
        }
        Ok::<_, Error>(())
    });
    let vector_response = client
        .add_vectors(ReceiverStream::new(vec_rx))
        .await?
        .into_inner();
    vector_sender.await??;
    let flush = client
        .flush(pipestream_search::pb::FlushRequest {})
        .await?
        .into_inner();
    println!(
        "{}",
        json!({
            "engine": "protomolt",
            "documents": doc_response.added,
            "vectors": vector_response.added,
            "ingest_ms": started.elapsed().as_secs_f64() * 1e3,
            "flush_path": flush.path,
        })
    );
    Ok(())
}

async fn health_protomolt() -> Result<(), Error> {
    let mut node = NodeServiceClient::connect(normalize_addr(&required("node")?)).await?;
    let node_health = node.health(HealthRequest {}).await?.into_inner();
    let mut coordinator =
        SearchServiceClient::connect(normalize_addr(&required("coordinator")?)).await?;
    let cluster = coordinator
        .cluster_health(ClusterHealthRequest {})
        .await?
        .into_inner();
    if cluster.targets.len() != 1 {
        return Err(format!(
            "challenge expected one shard, coordinator reports {}",
            cluster.targets.len()
        )
        .into());
    }
    println!(
        "{}",
        json!({
            "ready": true,
            "vectors": node_health.num_vectors,
            "documents": node_health.bm25_docs,
            "shards": cluster.targets.len(),
        })
    );
    Ok(())
}

fn http_client() -> HttpClient<HttpConnector, Full<Bytes>> {
    let mut connector = HttpConnector::new();
    connector.enforce_http(true);
    HttpClient::builder(TokioExecutor::new()).build(connector)
}

async fn http_json(
    client: &HttpClient<HttpConnector, Full<Bytes>>,
    method: Method,
    uri: String,
    body: Vec<u8>,
) -> Result<(StatusCode, Duration, Vec<u8>), Error> {
    let request = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))?;
    let started = Instant::now();
    let mut response = client.request(request).await?;
    let status = response.status();
    let mut first_payload = None;
    let mut first_hit = None;
    let mut bytes = Vec::new();
    while let Some(frame) = response.body_mut().frame().await {
        let frame = frame?;
        if let Some(data) = frame.data_ref() {
            if first_payload.is_none() && !data.is_empty() {
                first_payload = Some(started.elapsed());
            }
            bytes.extend_from_slice(data);
            if first_hit.is_none()
                && bytes
                    .windows(b"\"_id\":\"".len())
                    .any(|window| window == b"\"_id\":\"")
            {
                first_hit = Some(started.elapsed());
            }
        }
    }
    Ok((
        status,
        first_hit
            .or(first_payload)
            .unwrap_or_else(|| started.elapsed()),
        bytes,
    ))
}

fn ensure_success(status: StatusCode, body: &[u8]) -> Result<Value, Error> {
    let value: Value = serde_json::from_slice(body).map_err(|error| {
        format!(
            "HTTP {status}: invalid JSON: {error}; body={}",
            String::from_utf8_lossy(body)
        )
    })?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {value}").into());
    }
    Ok(value)
}

async fn ingest_opensearch() -> Result<(), Error> {
    let base = normalize_addr(&required("opensearch")?);
    let index = option("index").unwrap_or_else(|| "protomolt-challenge".into());
    let corpus = read_json_lines::<Document>(Path::new(&required("corpus")?))?;
    let dim = corpus.first().ok_or("empty corpus")?.vector.len();
    let client = http_client();
    let _ = http_json(
        &client,
        Method::DELETE,
        format!("{base}/{index}"),
        Vec::new(),
    )
    .await;
    let mapping = json!({
        "settings": {
            "index": {"knn": true, "number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
            "analysis": {"analyzer": {"challenge_ws": {"type": "custom", "tokenizer": "whitespace", "filter": ["lowercase"]}}}
        },
        "mappings": {"properties": {
            "text": {"type": "text", "analyzer": "challenge_ws"},
            "year": {"type": "integer"},
            "group": {"type": "keyword"},
            "parent": {"type": "long"},
            "vector": {"type": "knn_vector", "dimension": dim, "method": {
                "name": "hnsw", "engine": "lucene", "space_type": "innerproduct",
                "parameters": {"m": 16, "ef_construction": 100}
            }}
        }}
    });
    let (status, _, body) = http_json(
        &client,
        Method::PUT,
        format!("{base}/{index}"),
        serde_json::to_vec(&mapping)?,
    )
    .await?;
    ensure_success(status, &body)?;
    let pipeline = json!({
        "description": "Pinned unweighted RRF for the Protomolt challenge",
        "phase_results_processors": [{"score-ranker-processor": {"combination": {
            "technique": "rrf", "rank_constant": 60
        }}}]
    });
    let (status, _, body) = http_json(
        &client,
        Method::PUT,
        format!("{base}/_search/pipeline/protomolt-challenge-rrf"),
        serde_json::to_vec(&pipeline)?,
    )
    .await?;
    ensure_success(status, &body)?;

    let started = Instant::now();
    for batch in corpus.chunks(256) {
        let mut bulk = Vec::new();
        for doc in batch {
            serde_json::to_writer(
                &mut bulk,
                &json!({"index": {"_index": index, "_id": doc.id.to_string()}}),
            )?;
            bulk.push(b'\n');
            serde_json::to_writer(&mut bulk, doc)?;
            bulk.push(b'\n');
        }
        let (status, _, body) =
            http_json(&client, Method::POST, format!("{base}/_bulk"), bulk).await?;
        let response = ensure_success(status, &body)?;
        if response["errors"].as_bool() == Some(true) {
            return Err(format!("OpenSearch bulk item failed: {response}").into());
        }
    }
    let (status, _, body) = http_json(
        &client,
        Method::POST,
        format!("{base}/{index}/_refresh"),
        Vec::new(),
    )
    .await?;
    ensure_success(status, &body)?;
    println!(
        "{}",
        json!({
            "engine": "opensearch",
            "documents": corpus.len(),
            "ingest_ms": started.elapsed().as_secs_f64() * 1e3,
        })
    );
    Ok(())
}

fn filter_leaf(year: i64) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "year-filter".into(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                format!("year >= {year}"),
            )),
        })),
    }
}

fn search_leaf(workload: &Workload, dense: bool) -> SelectionQuery {
    let query = if dense {
        search_query::Query::Dense(DenseQuery {
            vector: workload.vector.clone(),
        })
    } else {
        search_query::Query::Lexical(LexicalQuery {
            text: workload.text.clone(),
            ..Default::default()
        })
    };
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: if dense { "dense" } else { "lexical" }.into(),
            query: Some(query),
        })),
    }
}

fn composite(
    operator: SelectionOperator,
    clauses: Vec<SelectionQuery>,
    scoring: Option<selection_score_strategy::Strategy>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(
            pipestream_search::pb::CompositeSearchStrategy {
                operator: operator as i32,
                clauses,
                scoring: scoring.map(|strategy| SelectionScoreStrategy {
                    strategy: Some(strategy),
                }),
            },
        )),
    }
}

fn protomolt_query(workload: &Workload, request_id: String) -> QueryRequest {
    let base = match workload.kind {
        WorkloadKind::Lexical | WorkloadKind::FilteredLexical => search_leaf(workload, false),
        WorkloadKind::Vector | WorkloadKind::FilteredVector => search_leaf(workload, true),
        WorkloadKind::Hybrid => composite(
            SelectionOperator::Or,
            vec![search_leaf(workload, true), search_leaf(workload, false)],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        ),
        WorkloadKind::CollapseVector => search_leaf(workload, true),
    };
    let selection = workload.min_year.map_or(base.clone(), |year| {
        composite(SelectionOperator::And, vec![base, filter_leaf(year)], None)
    });
    QueryRequest {
        request_id,
        k: workload.k,
        selection: Some(selection),
        ..Default::default()
    }
}

async fn one_protomolt(
    mut client: SearchServiceClient<tonic::transport::Channel>,
    workload: Workload,
    iteration: usize,
    concurrency: usize,
) -> Sample {
    let started = Instant::now();
    let mut first = None;
    let result: Result<(bool, Vec<u64>), Error> = async {
        if matches!(workload.kind, WorkloadKind::CollapseVector) {
            let response = client
                .search(SearchRequest {
                    request_id: format!("challenge-{}-{iteration}", workload.id),
                    vector: workload.vector.clone(),
                    k: workload.k,
                    collapse_parents: true,
                    ..Default::default()
                })
                .await?
                .into_inner();
            first = Some(started.elapsed());
            return Ok((
                true,
                response.hits.into_iter().map(|hit| hit.vector_id).collect(),
            ));
        }
        let request = protomolt_query(&workload, format!("challenge-{}-{iteration}", workload.id));
        let mut stream = client
            .query_stream(QueryStreamRequest {
                query: Some(request),
                timeout_ms: 30_000,
            })
            .await?
            .into_inner();
        let mut completion = None;
        while let Some(event) = stream.message().await? {
            match event.payload {
                Some(query_stream_response::Payload::Revision(revision)) => {
                    if first.is_none()
                        && !revision.hits.is_empty()
                        && QueryStreamPhase::try_from(revision.phase)? != QueryStreamPhase::Final
                    {
                        first = Some(started.elapsed());
                    }
                }
                Some(query_stream_response::Payload::Completion(done)) => {
                    completion = Some(done);
                }
                None => {}
            }
        }
        let done = completion.ok_or("QueryStream ended without completion")?;
        let ids = done
            .response
            .as_ref()
            .map(|response| response.hits.iter().map(|hit| hit.doc_id).collect())
            .unwrap_or_default();
        if first.is_none() && done.completed {
            first = Some(started.elapsed());
        }
        if !done.completed {
            return Err(format!("QueryStream incomplete: {}", done.error_message).into());
        }
        Ok((done.completed, ids))
    }
    .await;
    let latency = started.elapsed();
    match result {
        Ok((completed, hit_ids)) => Sample {
            record_type: "sample".into(),
            engine: "protomolt".into(),
            query_id: workload.id,
            workload: workload.kind.as_str().into(),
            iteration,
            concurrency,
            ok: true,
            completed,
            ttfh_ms: first.unwrap_or(latency).as_secs_f64() * 1e3,
            latency_ms: latency.as_secs_f64() * 1e3,
            hit_ids,
            error: String::new(),
        },
        Err(error) => Sample {
            record_type: "sample".into(),
            engine: "protomolt".into(),
            query_id: workload.id,
            workload: workload.kind.as_str().into(),
            iteration,
            concurrency,
            ok: false,
            completed: false,
            ttfh_ms: first.unwrap_or(latency).as_secs_f64() * 1e3,
            latency_ms: latency.as_secs_f64() * 1e3,
            hit_ids: Vec::new(),
            error: error.to_string(),
        },
    }
}

fn os_filter(workload: &Workload) -> Option<Value> {
    workload
        .min_year
        .map(|year| json!({"range": {"year": {"gte": year}}}))
}

fn os_query(workload: &Workload) -> (String, Value) {
    let lexical = || json!({"match": {"text": {"query": workload.text}}});
    let vector = || {
        let mut body = json!({"vector": workload.vector, "k": workload.k});
        if let Some(filter) = os_filter(workload) {
            body["filter"] = filter;
        }
        json!({"knn": {"vector": body}})
    };
    let query = match workload.kind {
        WorkloadKind::Lexical => lexical(),
        WorkloadKind::FilteredLexical => {
            json!({"bool": {"must": [lexical()], "filter": [os_filter(workload).unwrap()]}})
        }
        WorkloadKind::Vector | WorkloadKind::FilteredVector | WorkloadKind::CollapseVector => {
            vector()
        }
        WorkloadKind::Hybrid => json!({"hybrid": {
            "pagination_depth": workload.k.max(100),
            "queries": [lexical(), vector()]
        }}),
    };
    let mut body = json!({
        "size": workload.k,
        "_source": false,
        "track_total_hits": false,
        "query": query
    });
    if matches!(workload.kind, WorkloadKind::CollapseVector) {
        body["collapse"] = json!({"field": "parent"});
    }
    let suffix = if matches!(workload.kind, WorkloadKind::Hybrid) {
        "?search_pipeline=protomolt-challenge-rrf"
    } else {
        ""
    };
    (suffix.into(), body)
}

async fn one_opensearch(
    client: HttpClient<HttpConnector, Full<Bytes>>,
    base: String,
    index: String,
    workload: Workload,
    iteration: usize,
    concurrency: usize,
) -> Sample {
    let started = Instant::now();
    let result: Result<(Duration, Vec<u64>), Error> = async {
        let (suffix, query) = os_query(&workload);
        let (status, first, body) = http_json(
            &client,
            Method::POST,
            format!("{base}/{index}/_search{suffix}"),
            serde_json::to_vec(&query)?,
        )
        .await?;
        let response = ensure_success(status, &body)?;
        let hits = response["hits"]["hits"]
            .as_array()
            .ok_or("OpenSearch response omitted hits.hits")?;
        let ids = hits
            .iter()
            .map(|hit| {
                hit["_id"]
                    .as_str()
                    .ok_or("OpenSearch hit omitted string _id")?
                    .parse::<u64>()
                    .map_err(|error| error.into())
            })
            .collect::<Result<_, Error>>()?;
        Ok((first, ids))
    }
    .await;
    let latency = started.elapsed();
    match result {
        Ok((first, hit_ids)) => Sample {
            record_type: "sample".into(),
            engine: "opensearch".into(),
            query_id: workload.id,
            workload: workload.kind.as_str().into(),
            iteration,
            concurrency,
            ok: true,
            completed: true,
            ttfh_ms: first.as_secs_f64() * 1e3,
            latency_ms: latency.as_secs_f64() * 1e3,
            hit_ids,
            error: String::new(),
        },
        Err(error) => Sample {
            record_type: "sample".into(),
            engine: "opensearch".into(),
            query_id: workload.id,
            workload: workload.kind.as_str().into(),
            iteration,
            concurrency,
            ok: false,
            completed: false,
            ttfh_ms: latency.as_secs_f64() * 1e3,
            latency_ms: latency.as_secs_f64() * 1e3,
            hit_ids: Vec::new(),
            error: error.to_string(),
        },
    }
}

async fn run_engine(engine: &str) -> Result<(), Error> {
    let workloads = Arc::new(read_json_lines::<Workload>(Path::new(&required(
        "workload",
    )?))?);
    let output = PathBuf::from(required("output")?);
    let iterations: usize = parsed("iterations", 5)?;
    let warmup: usize = parsed("warmup", 1)?;
    let concurrency: usize = parsed("concurrency", 1)?;
    if concurrency == 0 {
        return Err("concurrency must be positive".into());
    }
    let mut writer = BufWriter::new(File::create(output)?);
    let proto = if engine == "protomolt" {
        Some(SearchServiceClient::connect(normalize_addr(&required("coordinator")?)).await?)
    } else {
        None
    };
    let os_client = (engine == "opensearch").then(http_client);
    let os_base = option("opensearch").map(|value| normalize_addr(&value));
    let os_index = option("index").unwrap_or_else(|| "protomolt-challenge".into());

    for index in 0..warmup {
        for workload in workloads.iter() {
            let sample = match engine {
                "protomolt" => {
                    one_protomolt(proto.as_ref().unwrap().clone(), workload.clone(), index, 1).await
                }
                "opensearch" => {
                    one_opensearch(
                        os_client.as_ref().unwrap().clone(),
                        os_base.as_ref().ok_or("--opensearch is required")?.clone(),
                        os_index.clone(),
                        workload.clone(),
                        index,
                        1,
                    )
                    .await
                }
                other => return Err(format!("unknown engine {other:?}").into()),
            };
            if !sample.ok {
                return Err(
                    format!("warmup failed for {}: {}", sample.query_id, sample.error).into(),
                );
            }
        }
    }

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for iteration in 0..iterations {
        for workload in workloads.iter().cloned() {
            let permit = Arc::clone(&semaphore).acquire_owned().await?;
            let proto = proto.clone();
            let client = os_client.clone();
            let base = os_base.clone();
            let index = os_index.clone();
            let engine = engine.to_string();
            tasks.spawn(async move {
                let _permit = permit;
                match engine.as_str() {
                    "protomolt" => {
                        one_protomolt(proto.unwrap(), workload, iteration, concurrency).await
                    }
                    "opensearch" => {
                        one_opensearch(
                            client.unwrap(),
                            base.unwrap(),
                            index,
                            workload,
                            iteration,
                            concurrency,
                        )
                        .await
                    }
                    _ => unreachable!(),
                }
            });
        }
    }
    let mut completed = 0usize;
    let requests = iterations * workloads.len();
    while let Some(sample) = tasks.join_next().await {
        let sample = sample?;
        completed += usize::from(sample.ok && sample.completed);
        serde_json::to_writer(&mut writer, &sample)?;
        writer.write_all(b"\n")?;
    }
    let elapsed = started.elapsed();
    let cell = Cell {
        record_type: "cell".into(),
        engine: engine.into(),
        concurrency,
        requests,
        completed,
        elapsed_ms: elapsed.as_secs_f64() * 1e3,
        qps: requests as f64 / elapsed.as_secs_f64(),
    };
    serde_json::to_writer(&mut writer, &cell)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    if completed != requests {
        return Err(format!("{engine}: only {completed}/{requests} requests completed").into());
    }
    println!("{}", serde_json::to_string(&cell)?);
    Ok(())
}

fn percentile(values: &mut [f64], fraction: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    let rank = ((values.len() as f64 * fraction).ceil() as usize).clamp(1, values.len());
    values[rank - 1]
}

fn ndcg(sample: &Sample, judgments: &[Judgment]) -> f64 {
    let gain: HashMap<u64, u32> = judgments
        .iter()
        .map(|judgment| (judgment.doc_id, judgment.gain))
        .collect();
    let dcg = sample
        .hit_ids
        .iter()
        .enumerate()
        .map(|(rank, id)| {
            let gain = f64::from(*gain.get(id).unwrap_or(&0));
            (2f64.powf(gain) - 1.0) / ((rank + 2) as f64).log2()
        })
        .sum::<f64>();
    let mut ideal: Vec<u32> = judgments.iter().map(|judgment| judgment.gain).collect();
    ideal.sort_by(|a, b| b.cmp(a));
    let idcg = ideal
        .into_iter()
        .take(sample.hit_ids.len())
        .enumerate()
        .map(|(rank, gain)| (2f64.powf(f64::from(gain)) - 1.0) / ((rank + 2) as f64).log2())
        .sum::<f64>();
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

fn recall(sample: &Sample, judgments: &[Judgment]) -> f64 {
    let relevant: std::collections::HashSet<u64> =
        judgments.iter().map(|judgment| judgment.doc_id).collect();
    let denominator = sample.hit_ids.len().min(relevant.len());
    if denominator == 0 {
        return 0.0;
    }
    sample
        .hit_ids
        .iter()
        .filter(|id| relevant.contains(id))
        .count() as f64
        / denominator as f64
}

#[derive(Serialize)]
struct ReportCell {
    engine: String,
    workload: String,
    concurrency: usize,
    samples: usize,
    ttfh_p50_ms: f64,
    ttfh_p95_ms: f64,
    ttfh_p99_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    mean_recall_at_k: f64,
    mean_ndcg_at_k: f64,
}

fn report() -> Result<(), Error> {
    let workload_path = PathBuf::from(required("workload")?);
    let workloads = read_json_lines::<Workload>(&workload_path)?;
    let qrels: HashMap<String, Workload> = workloads
        .into_iter()
        .map(|workload| (workload.id.clone(), workload))
        .collect();
    let inputs = required("results")?;
    let mut groups: BTreeMap<(String, String, usize), Vec<Sample>> = BTreeMap::new();
    let mut cells = Vec::new();
    for path in inputs.split(',') {
        for line in BufReader::new(File::open(path)?).lines() {
            let line = line?;
            let value: Value = serde_json::from_str(&line)?;
            match value["record_type"].as_str() {
                Some("sample") => {
                    let sample: Sample = serde_json::from_value(value)?;
                    groups
                        .entry((
                            sample.engine.clone(),
                            sample.workload.clone(),
                            sample.concurrency,
                        ))
                        .or_default()
                        .push(sample);
                }
                Some("cell") => cells.push(serde_json::from_value::<Cell>(value)?),
                _ => return Err(format!("{path}: unknown record_type").into()),
            }
        }
    }
    let mut report_cells = Vec::new();
    for ((engine, workload, concurrency), samples) in groups {
        if samples.iter().any(|sample| !sample.ok || !sample.completed) {
            return Err(format!("{engine}/{workload}/c{concurrency}: incomplete sample").into());
        }
        let mut ttfh: Vec<f64> = samples.iter().map(|sample| sample.ttfh_ms).collect();
        let mut latency: Vec<f64> = samples.iter().map(|sample| sample.latency_ms).collect();
        let recalls: Vec<f64> = samples
            .iter()
            .map(|sample| recall(sample, &qrels[&sample.query_id].judgments))
            .collect();
        let ndcgs: Vec<f64> = samples
            .iter()
            .map(|sample| ndcg(sample, &qrels[&sample.query_id].judgments))
            .collect();
        report_cells.push(ReportCell {
            engine,
            workload,
            concurrency,
            samples: samples.len(),
            ttfh_p50_ms: percentile(&mut ttfh.clone(), 0.50),
            ttfh_p95_ms: percentile(&mut ttfh.clone(), 0.95),
            ttfh_p99_ms: percentile(&mut ttfh, 0.99),
            latency_p50_ms: percentile(&mut latency.clone(), 0.50),
            latency_p95_ms: percentile(&mut latency.clone(), 0.95),
            latency_p99_ms: percentile(&mut latency, 0.99),
            mean_recall_at_k: recalls.iter().sum::<f64>() / recalls.len() as f64,
            mean_ndcg_at_k: ndcgs.iter().sum::<f64>() / ndcgs.len() as f64,
        });
    }
    let resources = option("resources")
        .map(|path| -> Result<Value, Error> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) })
        .transpose()?;
    let output = json!({
        "format": "protomolt-opensearch-challenge-report-v1",
        "workload_sha256": file_digest(&workload_path)?,
        "cells": report_cells,
        "throughput_cells": cells,
        "resources": resources,
    });
    if let Some(path) = option("output") {
        std::fs::write(path, serde_json::to_vec_pretty(&output)?)?;
    }
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn usage() -> &'static str {
    "usage: opensearch_challenge <generate|serve-mock|health-protomolt|ingest-protomolt|ingest-opensearch|run-protomolt|run-opensearch|report> [--key=value ...]"
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    match std::env::args().nth(1).as_deref() {
        Some("generate") => generate(),
        Some("serve-mock") => serve_mock().await,
        Some("health-protomolt") => health_protomolt().await,
        Some("ingest-protomolt") => ingest_protomolt().await,
        Some("ingest-opensearch") => ingest_opensearch().await,
        Some("run-protomolt") => run_engine("protomolt").await,
        Some("run-opensearch") => run_engine("opensearch").await,
        Some("report") => report(),
        _ => Err(usage().into()),
    }
}
