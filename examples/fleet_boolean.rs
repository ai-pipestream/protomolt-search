//! Boolean and allowlist shapes against a serving fleet root, for the
//! A/B binary rollout (docs/benchmarks/fleet-placement-2026-09.md, the
//! boolean tables). Each shape runs `--rounds` times through the given
//! coordinator; every round prints wall time, the profile's phase and
//! pruning counters, and the hits as `doc_id:score_bits:rank`, and each
//! shape ends with an FNV-1a digest over every round's hits so two
//! binaries' outputs compare exactly.
//!
//! ```text
//! fleet_boolean --coord=krick-1:19391 --tls-ca=... --tls-client-cert=... \
//!   --tls-client-key=... --bearer-token-file=... [--rounds=3] [--k=10] \
//!   [--embeddings=/work/court-corpus/embeddings-full.bin] [--dense-row=7] \
//!   [--shapes=lex-qualified+scotus,dense+year2018]
//! ```

use std::time::Instant;

use pipestream_search::analyzer::body_spec;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    filter_query, search_query, selection_query, Bm25SearchRequest, BooleanQuery, DenseQuery,
    FilterQuery, LexicalQuery, QueryRequest, QueryResponse, SearchQuery, SelectionQuery,
};
use pipestream_search::security::ToolClient;

fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn lexical(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                analysis: Some(body_spec()),
                ..Default::default()
            })),
        })),
    }
}

fn dense(id: &str, vector: Vec<f32>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector,
                ..Default::default()
            })),
        })),
    }
}

fn cel(id: &str, predicate: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(filter_query::Predicate::Cel(predicate.to_string())),
        })),
    }
}

fn boolean(must: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

struct Digest(u64);

impl Digest {
    fn add(&mut self, doc_id: u64, score_bits: u32, rank: u32) {
        for b in doc_id
            .to_le_bytes()
            .into_iter()
            .chain(score_bits.to_le_bytes())
            .chain(rank.to_le_bytes())
        {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

fn fnv_offset() -> u64 {
    0xcbf29ce484222325
}

type Client = SearchServiceClient<pipestream_search::security::PublicChannel>;

async fn run_boolean(
    client: &mut Client,
    name: &str,
    selection: SelectionQuery,
    k: u32,
    rounds: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut digest = Digest(fnv_offset());
    let mut total_hits = 0usize;
    for round in 1..=rounds {
        let started = Instant::now();
        let response: QueryResponse = client
            .query(QueryRequest {
                request_id: String::new(),
                k,
                selection_k: 0,
                selection: Some(selection.clone()),
                boosts: Vec::new(),
                scorer: None,
                profile: true,
                ..Default::default()
            })
            .await?
            .into_inner();
        let wall = started.elapsed().as_secs_f64() * 1000.0;
        let mut hits = String::new();
        for hit in &response.hits {
            digest.add(hit.doc_id, hit.score.to_bits(), hit.rank);
            total_hits += 1;
            if !hits.is_empty() {
                hits.push(',');
            }
            hits.push_str(&format!(
                "{}:{:08x}:{}",
                hit.doc_id,
                hit.score.to_bits(),
                hit.rank
            ));
        }
        let (sel, tot, segs, shards) = match &response.profile {
            Some(p) => (
                format!("{:.1}", p.selection_ms),
                format!("{:.1}", p.total_ms),
                format!("{}/{}", p.segments_total, p.segments_skipped),
                format!("{}/{}", p.shards_total, p.shards_skipped),
            ),
            None => (
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
            ),
        };
        println!(
            "shape={name} round={round} wall_ms={wall:.1} selection_ms={sel} total_ms={tot} segments={segs} shards={shards} executed={} hits={hits}",
            response.executed
        );
    }
    println!(
        "digest shape={name} rounds={rounds} hits={total_hits} fnv={:016x}",
        digest.0
    );
    Ok(())
}

async fn run_allowlist(
    client: &mut Client,
    name: &str,
    text: &str,
    filter: &str,
    k: u32,
    rounds: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut digest = Digest(fnv_offset());
    let mut total_hits = 0usize;
    for round in 1..=rounds {
        let started = Instant::now();
        let response = client
            .bm25_search(Bm25SearchRequest {
                text: text.to_string(),
                k,
                filter: filter.to_string(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await?
            .into_inner();
        let wall = started.elapsed().as_secs_f64() * 1000.0;
        let mut hits = String::new();
        for (index, hit) in response.hits.iter().enumerate() {
            digest.add(hit.doc_id, hit.score.to_bits(), index as u32 + 1);
            total_hits += 1;
            if !hits.is_empty() {
                hits.push(',');
            }
            hits.push_str(&format!(
                "{}:{:08x}:{}",
                hit.doc_id,
                hit.score.to_bits(),
                index + 1
            ));
        }
        println!("shape={name} round={round} wall_ms={wall:.1} hits={hits}");
    }
    println!(
        "digest shape={name} rounds={rounds} hits={total_hits} fnv={:016x}",
        digest.0
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let security = ToolClient::from_env_args()?;
    let coord = security.url(&arg("coord", "127.0.0.1:59291"));
    let rounds: u32 = arg("rounds", "3").parse()?;
    let k: u32 = arg("k", "10").parse()?;
    let embeddings = arg("embeddings", "/work/court-corpus/embeddings-full.bin");
    let dense_row: usize = arg("dense-row", "7").parse()?;
    let shapes: Vec<String> = arg(
        "shapes",
        "lex-grandfathered+dense,lex-qualified+dense,lex-qualified+scotus,\
         lex-qualified+year2018,dense+scotus,dense+year2018,\
         allowlist-scotus,allowlist-year2018",
    )
    .split(',')
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
    .collect();

    let (dim, mut reader) =
        pipestream_search::demo::court::EmbeddingReader::open(std::path::Path::new(&embeddings))?;
    let record = reader
        .nth(dense_row)
        .ok_or_else(|| format!("{embeddings}: no record {dense_row}"))??;
    let vector = record.vector;
    println!("probe dense row {dense_row} of {embeddings} (dim {dim})");

    let mut client =
        SearchServiceClient::with_interceptor(security.connect(&coord).await?, security.bearer());

    for shape in &shapes {
        match shape.as_str() {
            "lex-grandfathered+dense" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![
                        lexical("l", "grandfathered status"),
                        dense("v", vector.clone()),
                    ]),
                    k,
                    rounds,
                )
                .await?
            }
            "lex-qualified+dense" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![
                        lexical("l", "qualified immunity"),
                        dense("v", vector.clone()),
                    ]),
                    k,
                    rounds,
                )
                .await?
            }
            "lex-qualified+scotus" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![
                        lexical("l", "qualified immunity"),
                        cel("f", "court == \"scotus\""),
                    ]),
                    k,
                    rounds,
                )
                .await?
            }
            "lex-qualified+year2018" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![
                        lexical("l", "qualified immunity"),
                        cel("f", "year >= 2018"),
                    ]),
                    k,
                    rounds,
                )
                .await?
            }
            "dense+scotus" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![
                        dense("v", vector.clone()),
                        cel("f", "court == \"scotus\""),
                    ]),
                    k,
                    rounds,
                )
                .await?
            }
            "dense+year2018" => {
                run_boolean(
                    &mut client,
                    shape,
                    boolean(vec![dense("v", vector.clone()), cel("f", "year >= 2018")]),
                    k,
                    rounds,
                )
                .await?
            }
            "allowlist-scotus" => {
                run_allowlist(
                    &mut client,
                    shape,
                    "qualified immunity",
                    "court == \"scotus\"",
                    k,
                    rounds,
                )
                .await?
            }
            "allowlist+year2018" | "allowlist-year2018" => {
                run_allowlist(
                    &mut client,
                    shape,
                    "qualified immunity",
                    "year >= 2018",
                    k,
                    rounds,
                )
                .await?
            }
            other => return Err(format!("no such shape: {other}").into()),
        }
    }
    Ok(())
}
