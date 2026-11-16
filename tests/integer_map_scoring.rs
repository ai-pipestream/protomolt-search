mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    harness::start_relay,
    node::NodeConfig,
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
};
use tonic::Request;

const SIGNED: [Option<i64>; 8] = [
    None,
    Some(0),
    Some(-1),
    Some(i64::MIN),
    Some(i64::MAX),
    Some((1 << 53) + 1),
    Some(i64::MIN + 1),
    Some(1),
];
const UNSIGNED: [Option<u64>; 8] = [
    None,
    Some(0),
    Some(1),
    Some(u64::MAX),
    Some(1 << 63),
    Some((1 << 53) + 1),
    Some(u64::MAX - 1),
    Some(1 << 53),
];
const KEY: &str = "zz'\\\"雪";
fn stages(column: &str, key: &str) -> Vec<pb::ScoreStage> {
    [
        pb::ScoreOp::AddLinear,
        pb::ScoreOp::MultLog,
        pb::ScoreOp::MultExpDecay,
    ]
    .into_iter()
    .map(|op| pb::ScoreStage {
        op: op as i32,
        map_key: Some(key.into()),
        column: column.into(),
        weight: if op == pb::ScoreOp::AddLinear {
            -2e-19
        } else {
            0.4
        },
        origin: 0.0,
        scale: 9_223_372_036_854_775_808.0,
        ..Default::default()
    })
    .collect()
}
fn input(column: &str, row: usize) -> Option<f64> {
    // Decimal parsing is an independent oracle for the declared rounding.
    if column == "signed" {
        SIGNED[row % 8].map(|v| v.to_string().parse().unwrap())
    } else {
        UNSIGNED[row % 8].map(|v| v.to_string().parse().unwrap())
    }
}
fn effect(index: usize, x: f64) -> f64 {
    match index {
        0 => -2e-19 * x,
        1 => 1.0 + 0.4 * (1.0 + x.max(0.0)).ln(),
        _ => (-(x.abs()) / 9_223_372_036_854_775_808.0).exp(),
    }
}
fn query(column: &str, key: &str) -> pb::Bm25SearchRequest {
    pb::Bm25SearchRequest {
        text: "word".into(),
        k: 32,
        analysis: Some(body_spec()),
        score_stages: stages(column, key),
        explain: true,
        projections: vec![pb::NamedProjection {
            name: "row".into(),
            expression: "row".into(),
        }],
        ..Default::default()
    }
}
async fn verify(address: String) {
    let coord = CoordinatorServiceImpl::new(vec![address.clone()])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    for column in ["signed", "unsigned"] {
        for key in ["", KEY] {
            let response = coord
                .bm25_search(Request::new(query(column, key)))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.hits.len(), 16);
            for hit in response.hits {
                let Some(pb::projected_value::Value::IntValue(row)) = hit.projected[0].value else {
                    panic!("row absent")
                };
                let explain = hit.explain.unwrap();
                let mut score = explain.bm25;
                assert_eq!(explain.stages.len(), 3);
                for (index, stage) in explain.stages.iter().enumerate() {
                    assert_eq!(stage.map_key.as_deref(), Some(key));
                    let x = input(column, row as usize);
                    assert_eq!(stage.present, x.is_some());
                    if let Some(x) = x {
                        assert_eq!(stage.input, x);
                        let contribution = effect(index, x);
                        assert_eq!(stage.contribution, contribution);
                        score = if index == 0 {
                            score + contribution
                        } else {
                            score * contribution
                        };
                    }
                    assert_eq!(stage.output, score);
                }
                assert_eq!(hit.score, score as f32);
            }
            let values = coord
                .fetch_values(
                    &(0..16).collect::<Vec<_>>(),
                    &[pb::CompiledProjection {
                        name: "row".into(),
                        expr: Some(pipestream_search::cel::compile_value("row").unwrap()),
                    }],
                    &stages(column, key),
                )
                .await
                .unwrap();
            for row in 0..16 {
                for i in 0..3 {
                    let Some(pb::projected_value::Value::IntValue(original)) =
                        values.rows[&(row as u64)][0].value
                    else {
                        panic!("row missing after compaction")
                    };
                    let expected = input(column, original as usize).map(|x| effect(i, x));
                    assert_eq!(values.stage_rows[i].get(&(row as u64)).copied(), expected);
                }
            }
        }
        let result = coord
            .query(Request::new(pb::QueryRequest {
                k: 32,
                selection_k: 32,
                selection: Some(pb::SelectionQuery {
                    node: Some(pb::selection_query::Node::Search(pb::SearchQuery {
                        id: "words".into(),
                        query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                            text: "word".into(),
                            analysis: Some(body_spec()),
                            ..Default::default()
                        })),
                    })),
                }),
                projections: vec![pb::NamedProjection {
                    name: "row".into(),
                    expression: "row".into(),
                }],
                scorer: Some(pb::CompositeScorer {
                    operation: pb::CompositeScoreOperation::WeightedSum as i32,
                    dimensions: vec![pb::ScoreDimension {
                        id: "map".into(),
                        source: Some(pb::ScoreSignal {
                            source: Some(pb::score_signal::Source::BoundedValue(
                                stages(column, "")[0].clone(),
                            )),
                        }),
                        normalization: pb::ScoreNormalization::None as i32,
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.hits.len(), 16);
        for hit in result.hits {
            let Some(pb::projected_value::Value::IntValue(row)) = hit.projected[0].value else {
                panic!("row absent")
            };
            let expected = input(column, row as usize).map(|x| effect(0, x));
            assert_eq!(hit.dimensions[0].raw, expected);
            assert_eq!(hit.score, expected.unwrap_or(0.0) as f32);
        }
        assert!(coord
            .bm25_search(Request::new(query(column, "missing")))
            .await
            .is_err());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_map_score_values_explain_and_fetch_survive_relay_reopen_and_compaction() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 92371));
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-score-segments-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = NodeConfig {
        index_path: Some(dir.join("index.tv")),
        wal: true,
        wal_buckets: 2,
        seal_tail_docs: 3,
        map_integer_fields: vec!["signed".into()],
        map_unsigned_integer_fields: vec!["unsigned".into()],
        integer_fields: vec!["row".into()],
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    };
    let (addr, handle) = common::start_empty_node(config.clone()).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            8, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    for row in 0..16 {
        let keys = if row < 8 {
            vec!["", KEY]
        } else {
            vec!["", "a", KEY]
        };
        client
            .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                text: vec!["word"; 1 + row % 3].join(" "),
                analysis: Some(body_spec()),
                integers: vec![pb::IntegerValue {
                    field: "row".into(),
                    value: row as i64,
                }],
                map_integers: keys
                    .iter()
                    .flat_map(|key| {
                        SIGNED[row % 8].map(|value| pb::MapIntegerEntry {
                            field: "signed".into(),
                            key: (*key).into(),
                            value,
                        })
                    })
                    .collect(),
                map_unsigned_integers: keys
                    .iter()
                    .flat_map(|key| {
                        UNSIGNED[row % 8].map(|value| pb::MapUnsignedIntegerEntry {
                            field: "unsigned".into(),
                            key: (*key).into(),
                            value,
                        })
                    })
                    .collect(),
                ..Default::default()
            }]))
            .await
            .unwrap();
        client
            .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                materialized_rows: Vec::new(),
                dim: 8,
                vectors: common::unit_vectors(1, 8, 511 + row as u64),
            }]))
            .await
            .unwrap();
        if row == 7 {
            client.flush(pb::FlushRequest {}).await.unwrap();
        }
    }
    verify(addr.clone()).await;
    let (empty, empty_h) = common::start_empty_node(NodeConfig {
        slot_offset: 16,
        map_integer_fields: vec!["signed".into()],
        map_unsigned_integer_fields: vec!["unsigned".into()],
        ..Default::default()
    })
    .await;
    NodeServiceClient::connect(empty.clone())
        .await
        .unwrap()
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            8, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    let (relay, _, rh) = start_relay(vec![addr.clone(), empty]).await;
    let (root, _, root_h) = start_relay(vec![relay]).await;
    verify(root).await;
    root_h.abort();
    rh.abort();
    empty_h.abort();
    client.flush(pb::FlushRequest {}).await.unwrap();
    handle.abort();
    let _ = handle.await;
    for compact in [true, false] {
        let (addr, handle) = common::start_opened_node(config.clone()).await;
        if compact {
            NodeServiceClient::connect(addr.clone())
                .await
                .unwrap()
                .compact_shard(pb::CompactShardRequest {
                    work_dir: dir.join("compact").display().to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        verify(addr).await;
        handle.abort();
        let _ = handle.await;
    }
    std::fs::remove_dir_all(dir).unwrap();
}

mod pruning {
    use super::*;
    use pipestream_search::{
        bm25::{self, Bm25Params, CorpusStats},
        postings::{AnalyzedDoc, Bm25Reader, Bm25Store, ColumnKind::*},
        scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp},
    };
    struct Columns<'a>(&'a Bm25Reader);
    impl NumericRead for Columns<'_> {
        fn value(&self, c: usize, d: u32) -> Option<f64> {
            self.0.numeric_value(c, d)
        }
        fn int_value(&self, c: usize, d: u32) -> Option<i64> {
            self.0.integer_value(c, d)
        }
        fn uint_value(&self, c: usize, d: u32) -> Option<u64> {
            self.0.unsigned_integer_value(c, d)
        }
        fn map_value(&self, c: usize, k: u32, d: u32) -> Option<f64> {
            self.0.map_numeric_value(c, k, d)
        }
        fn map_int_value(&self, c: usize, k: u32, d: u32) -> Option<i64> {
            self.0.map_integer_value(c, k, d)
        }
        fn map_uint_value(&self, c: usize, k: u32, d: u32) -> Option<u64> {
            self.0.map_unsigned_integer_value(c, k, d)
        }
        fn geo_value(&self, c: usize, d: u32) -> Option<(f64, f64)> {
            self.0.geo_value(c, d)
        }
        fn facet_ord(&self, c: usize, d: u32) -> Option<u32> {
            self.0.facet_ord(c, d)
        }
        fn map_facet_value_ord(&self, c: usize, k: u32, d: u32) -> Option<u32> {
            self.0.map_facet_value_ord(c, k, d)
        }
    }
    fn build_integer_map_store(path: &std::path::Path) -> CorpusStats {
        let mut store = Bm25Store::with_text_fields(&["body"])
            .with_columns(MapInteger, &["signed"])
            .with_columns(MapUnsignedInteger, &["unsigned"]);
        let n = 3000u32;
        let mut total = 0;
        for row in 0..n {
            let mut terms = vec![("a".into(), 1 + row % 7, vec![])];
            if row % 3 == 0 {
                terms.push(("b".into(), 1, vec![]));
            }
            if row % 61 == 0 {
                terms.push(("c".into(), 1, vec![]));
            }
            let len = terms.iter().map(|(_, tf, _)| tf).sum();
            total += u64::from(len);
            store.add_document(row, ".".into(), AnalyzedDoc::body(terms, len));
            if let Some(value) = SIGNED[row as usize % 8] {
                store.set_map_integer(0, row, "", value).unwrap();
            }
            if let Some(value) = UNSIGNED[row as usize % 8] {
                store.set_map_unsigned_integer(0, row, "", value).unwrap();
            }
        }
        store.save(path).unwrap();
        CorpusStats {
            doc_count: u64::from(n),
            total_doc_length: total,
            dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
        }
    }

    fn resolve_map_col(r: &Bm25Reader, name: &str) -> (ColumnRef, (f64, f64)) {
        let pf = |s: String| s.parse::<f64>().unwrap();
        if name == "signed" {
            let key_ord = r.map_integer_key_ord(0, "").unwrap();
            let (lo, hi) = r.map_integer_key_min_max(0, key_ord).unwrap();
            return (
                ColumnRef::MapIntegerKey { column: 0, key_ord },
                (pf(lo.to_string()), pf(hi.to_string())),
            );
        }
        let key_ord = r.map_unsigned_integer_key_ord(0, "").unwrap();
        let (lo, hi) = r.map_unsigned_integer_key_min_max(0, key_ord).unwrap();
        (
            ColumnRef::MapUnsignedIntegerKey { column: 0, key_ord },
            (pf(lo.to_string()), pf(hi.to_string())),
        )
    }

    fn assert_map_stage_bounds(
        c: &Columns<'_>,
        name: &str,
        col: ColumnRef,
        mm: (f64, f64),
        ops: &[StageOp],
    ) {
        for (i, &op) in ops.iter().enumerate() {
            let stage = Stage {
                op,
                column: Some(col),
                min_max: mm,
            };
            let chain = ScoreChain {
                stages: vec![stage],
            };
            for row in 0..8 {
                let x = input(name, row);
                assert_eq!(stage.input(row as u32, c), x);
                let contrib = x.map(|x| if i == 3 { 2e-19 * x } else { effect(i, x) });
                assert_eq!(stage.contribution(row as u32, c), contrib);
                for base in [-10.0, 0.0, 20.0] {
                    let expected =
                        contrib
                            .map_or(base, |v| if i == 0 || i == 3 { base + v } else { base * v });
                    let actual = chain.eval(base, row as u32, c);
                    assert_eq!(actual, expected);
                    assert!(chain.bound(20.0) >= actual);
                }
            }
        }
    }

    #[test]
    fn integer_map_bounds_preserve_exhaustive_top_k_with_seeded_floors() {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("map-score-prune-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("score.bm25");
        let stats = build_integer_map_store(&path);
        let reader = Bm25Reader::open(&path).unwrap();
        let columns = Columns(&reader);
        let body = reader.field(0);
        let terms = ["a", "b", "c"].map(str::to_owned);
        let p = Bm25Params::default();
        let ops = [
            StageOp::AddLinear { weight: -2e-19 },
            StageOp::MultLog { weight: 0.4 },
            StageOp::MultExpDecay {
                origin: 0.0,
                scale: 9_223_372_036_854_775_808.0,
            },
            StageOp::AddLinear { weight: 2e-19 },
        ];
        let sig = |hits: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
            hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
        };
        for name in ["signed", "unsigned"] {
            let (col, min_max) = resolve_map_col(&reader, name);
            assert_map_stage_bounds(&columns, name, col, min_max, &ops);
            for selected in [ops.to_vec(), ops[..3].to_vec(), vec![ops[3]]] {
                let stages = selected
                    .into_iter()
                    .map(|op| Stage {
                        op,
                        column: Some(col),
                        min_max,
                    })
                    .collect();
                let chain = ScoreChain { stages };
                let ctx = Some((&chain, &columns as &dyn NumericRead));
                for k in [1, 5, 50] {
                    let q = bm25::TopK::new(k).chain(ctx);
                    let expected = bm25::top_k_exhaustive(&body, &terms, &stats, p, q);
                    for floor in [f64::NEG_INFINITY, expected.last().unwrap().score] {
                        let q = bm25::TopK::new(k).floor(floor).chain(ctx);
                        let actual = bm25::top_k(&body, &terms, &stats, p, q);
                        assert_eq!(sig(&actual), sig(&expected));
                    }
                }
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn unsupported_and_malformed_map_operations_refuse_on_empty_nodes() {
    let (addr, handle) = common::start_empty_node(NodeConfig::default()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let valid = stages("unsigned", "")[0].clone();
    let mut unspecified = valid.clone();
    unspecified.op = pb::ScoreOp::Unspecified as i32;
    let mut unnamed = valid.clone();
    unnamed.column.clear();
    let mut geo = valid.clone();
    geo.op = pb::ScoreOp::MultGeoDecayHaversine as i32;
    geo.scale = 1.;
    let mut bad = valid.clone();
    bad.weight = f64::NAN;
    for stage in [unspecified, unnamed, geo, bad] {
        assert_eq!(
            client
                .fetch_values(pb::FetchValuesRequest {
                    stages: vec![stage.clone()],
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            client
                .bm25_query(pb::Bm25QueryRequest {
                    score_stages: vec![stage.clone()],
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            client
                .bm25_rescore(pb::Bm25RescoreRequest {
                    score_stages: vec![stage],
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let accepted = client
        .fetch_values(pb::FetchValuesRequest {
            stages: vec![valid],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.stage_columns_known, vec![false]);
    handle.abort();
    let _ = handle.await;
}
