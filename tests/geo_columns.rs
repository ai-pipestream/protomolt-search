//! Geo-point column and geo-filter acceptance tests
//! : kind 5 through the v7 column table with a
//! bounding box validated against a full scan, bbox and radius filters
//! that remove documents exactly (edges inside, absence out) without
//! disturbing any block-max bound, and distance-decay score stages that
//! prune bitwise identically to the exhaustive oracle.

mod common;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::geo::{self, GeoFilters, GeoMetric, GeoRegion};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25SearchRequest, GeoBbox, GeoFilter, GeoPointValue, GeoRadius,
    QueryField, ScoreOp, ScoreStage,
};
use pipestream_search::postings::{
    AnalyzedDoc, Bm25Reader, Bm25Store, ColumnKind::*, SpillBuilder,
};
use pipestream_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's geo points at ingest: (field, lat, lon) triples.
type Points = &'static [(&'static str, f64, f64)];

/// The controlled corpus: nine documents over three shards with a
/// "courthouse" geo column. Shard 2 declares NO geo field — the
/// heterogeneous-fleet case, whose documents hold no location and are
/// therefore inside no region, exactly and not by degradation.
///
/// The coordinates are chosen so every boundary rule can be hand
/// computed on the sphere `geo::EARTH_RADIUS_M` pins:
///
/// - d0/d1/d2 sit on three CORNERS of the unit box `[0,1] x [0,1]`, so
///   one filter pins all four inclusive edges at once,
/// - d1 and d2 are each exactly one degree of arc from the origin
///   `(0, 0)` (at the equator a degree of longitude is a degree of
///   latitude), so "distance exactly == meters is inside" has two
///   witnesses,
/// - d4 sits ON the north pole, where `cos(lat)` is zero,
/// - d6 sits at longitude 179.9, just west of the antimeridian, which
///   is reachable by an ordinary `[179, 180]` box and NOT by a
///   wraparound box (this increment refuses those),
/// - d5 has no point at all, and d7/d8 live on the column-less shard.
///
/// df("rust") = 7 (d0, d1, d2, d3, d5, d6, d7).
const SHARD_DOCS: [&[(&str, Points)]; 3] = [
    &[
        ("rust search rust fast", &[("courthouse", 0.0, 0.0)]),
        ("vector search rust", &[("courthouse", 0.0, 1.0)]),
        ("rust rust", &[("courthouse", 1.0, 0.0)]),
    ],
    &[
        ("search engines love rust", &[("courthouse", 10.0, 10.0)]),
        ("vector vector vector", &[("courthouse", 90.0, 45.0)]),
        ("rust fast", &[]),
        ("rust near the dateline", &[("courthouse", -45.0, 179.9)]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

/// Global slot offsets, matching `SHARD_DOCS`' shard sizes.
const OFFSETS: [u64; 3] = [0, 3, 7];

async fn add_documents_geo(
    addr: &str,
    docs: &[(&str, Points)],
) -> Result<pipestream_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    // Capacity above the largest batch this file sends: the whole
    // stream is queued BEFORE `add_documents` starts draining it, so a
    // channel smaller than the batch deadlocks on the send.
    let (tx, rx) = mpsc::channel(64);
    for (text, points) in docs {
        tx.send(AddDocumentsRequest {
            nlp_materialization: String::new(),
            unsigned_integers: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            derived_fingerprint: String::new(),
            original_source: None,
            source_chunk_ordinal: None,
            identity: None,
            collection: String::new(),
            cased_field: String::new(),
            sentence_fields: Vec::new(),
            materialize: None,
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            timestamps: Vec::new(),
            geo_points: points
                .iter()
                .map(|(field, lat, lon)| GeoPointValue {
                    field: field.to_string(),
                    lat: *lat,
                    lon: *lon,
                })
                .collect(),
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
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner())
}

/// Three shards, the geo table declared on shards 0 and 1 only.
async fn start_geo_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, &offset) in OFFSETS.iter().enumerate() {
        let geo_fields = if i < 2 {
            vec!["courthouse".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: offset,
            analysis_addr: Some(analysis.to_string()),
            geo_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_geo(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn bbox_filter(column: &str, min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64) -> GeoFilter {
    GeoFilter {
        column: column.to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
            min_lat,
            max_lat,
            min_lon,
            max_lon,
        })),
    }
}

fn radius_filter(column: &str, lat: f64, lon: f64, meters: f64, metric: GeoMetric) -> GeoFilter {
    GeoFilter {
        column: column.to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Radius(
            GeoRadius {
                lat,
                lon,
                meters,
                metric: match metric {
                    GeoMetric::Haversine => pipestream_search::pb::GeoMetric::Haversine as i32,
                    GeoMetric::Manhattan => pipestream_search::pb::GeoMetric::Manhattan as i32,
                },
            },
        )),
    }
}

fn geo_stage(column: &str, op: ScoreOp, lat: f64, lon: f64, scale: f64) -> ScoreStage {
    ScoreStage {
        op: op as i32,
        column: column.to_string(),
        weight: 0.0,
        origin: 0.0,
        scale,
        origin_lat: lat,
        origin_lon: lon,
        map_key: None,
    }
}

fn ids(hits: &[Bm25Hit]) -> Vec<u64> {
    let mut v: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    v.sort_unstable();
    v
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[path = "geo_columns/distributed_decay.rs"]
mod distributed_decay;
#[path = "geo_columns/distributed_filters.rs"]
mod distributed_filters;
#[path = "geo_columns/filter_and_ingest_refusals.rs"]
mod filter_and_ingest_refusals;
#[path = "geo_columns/pruned_scoring.rs"]
mod pruned_scoring;
#[path = "geo_columns/storage_and_corruption.rs"]
mod storage_and_corruption;
