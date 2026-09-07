mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    bm25::{self, Bm25Params, CorpusStats},
    coordinator::CoordinatorServiceImpl,
    harness::start_relay,
    node::{Layout, NodeConfig},
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
    postings::{AnalyzedDoc, Bm25Reader, Bm25Store},
    scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp},
};
use tonic::Request;

struct Columns<'a>(&'a Bm25Reader);
impl NumericRead for Columns<'_> {
    fn uint_value(&self, column: usize, doc: u32) -> Option<u64> {
        self.0.unsigned_integer_value(column, doc)
    }
    fn int_value(&self, column: usize, doc: u32) -> Option<i64> {
        self.0.integer_value(column, doc)
    }
    fn value(&self, column: usize, doc: u32) -> Option<f64> {
        self.0.numeric_value(column, doc)
    }
    fn map_value(&self, c: usize, k: u32, d: u32) -> Option<f64> {
        self.0.map_numeric_value(c, k, d)
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
const VALUES: [Option<u64>; 8] = [
    None,
    Some(0),
    Some(1),
    Some((1 << 53) + 1),
    Some(i64::MAX as u64),
    Some(1 << 63),
    Some(u64::MAX - 1),
    Some(u64::MAX),
];
fn ops() -> Vec<StageOp> {
    vec![
        StageOp::AddLinear { weight: -2e-19 },
        StageOp::MultLog { weight: 0.4 },
        StageOp::MultExpDecay {
            origin: 9_223_372_036_854_775_808.0,
            scale: 9_223_372_036_854_775_808.0,
        },
    ]
}
fn stages() -> Vec<pb::ScoreStage> {
    vec![
        pb::ScoreStage {
            column: "counter".into(),
            operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                pb::ScoreOp::AddLinear as i32,
            )),
            weight: -2e-19,
            ..Default::default()
        },
        pb::ScoreStage {
            column: "counter".into(),
            operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                pb::ScoreOp::MultLog as i32,
            )),
            weight: 0.4,
            ..Default::default()
        },
        pb::ScoreStage {
            column: "counter".into(),
            operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                pb::ScoreOp::MultExpDecay as i32,
            )),
            origin: 9_223_372_036_854_775_808.0,
            scale: 9_223_372_036_854_775_808.0,
            ..Default::default()
        },
    ]
}
// Decimal parsing is independent of the integer-to-float cast at the read site.
fn input(value: u64) -> f64 {
    value.to_string().parse().unwrap()
}
fn contribution(stage: &pb::ScoreStage, value: u64) -> f64 {
    let x = input(value);
    let Some(pb::score_stage::Operation::Op(op)) = stage.operation else {
        panic!("expected scalar operation")
    };
    match pb::ScoreOp::try_from(op).unwrap() {
        pb::ScoreOp::AddLinear => stage.weight * x,
        pb::ScoreOp::MultLog => 1.0 + stage.weight * (1.0 + x.max(0.0)).ln(),
        pb::ScoreOp::MultExpDecay => (-((x - stage.origin).abs()) / stage.scale).exp(),
        _ => unreachable!(),
    }
}
#[test]
fn unsigned_chain_bounds_and_pruning_preserve_the_declared_float_arithmetic() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("uint_score_pruning_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let n = 3000u32;
    let mut total = 0u64;
    let mut store = Bm25Store::with_fields(&["body"]).with_unsigned_integers(&["counter", "empty"]);
    for doc in 0..n {
        let mut terms = vec![("a".into(), 1 + doc % 7, vec![])];
        if doc % 3 == 0 {
            terms.push(("b".into(), 1, vec![]));
        }
        if doc % 61 == 0 {
            terms.push(("c".into(), 1, vec![]));
        }
        let len = terms.iter().map(|(_, tf, _)| tf).sum();
        total += u64::from(len);
        store.add_document(doc, ".".into(), AnalyzedDoc::body(terms, len));
        if let Some(v) = VALUES[doc as usize % VALUES.len()] {
            store.set_unsigned_integer(0, doc, v);
        }
    }
    let path = dir.join("score.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let cols = Columns(&reader);
    let body = reader.field(0);
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let stats = CorpusStats {
        doc_count: u64::from(n),
        total_doc_length: total,
        dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
    };
    let mut all = ops();
    all.push(StageOp::AddLinear { weight: 2e-19 });
    for op in all.iter().copied() {
        let stage = Stage {
            op,
            column: Some(ColumnRef::UnsignedInteger(0)),
            min_max: (0.0, input(u64::MAX)),
        };
        let wire = match op {
            StageOp::AddLinear { weight } => pb::ScoreStage {
                weight,
                ..stages()[0].clone()
            },
            StageOp::MultLog { .. } => stages()[1].clone(),
            _ => stages()[2].clone(),
        };
        for (doc, value) in VALUES.into_iter().enumerate() {
            assert_eq!(
                stage.input(doc as u32, &cols).map(f64::to_bits),
                value.map(|v| input(v).to_bits())
            );
            assert_eq!(
                stage.contribution(doc as u32, &cols).map(f64::to_bits),
                value.map(|v| contribution(&wire, v).to_bits())
            );
            let chain = ScoreChain {
                stages: vec![stage],
            };
            for base in [-20.0, -1.0, 0.0, 0.25, 2.0, 20.0] {
                let expected = value.map_or(base, |v| {
                    if stage.is_additive() {
                        base + contribution(&wire, v)
                    } else {
                        base * contribution(&wire, v)
                    }
                });
                let actual = chain.eval(base, doc as u32, &cols);
                assert_eq!(actual.to_bits(), expected.to_bits());
                assert!(chain.bound(20.0) >= actual);
            }
        }
    }
    for ops in [all, ops()] {
        let chain = ScoreChain {
            stages: ops
                .into_iter()
                .map(|op| Stage {
                    op,
                    column: Some(ColumnRef::UnsignedInteger(0)),
                    min_max: (0.0, input(u64::MAX)),
                })
                .collect(),
        };
        let ctx = Some((&chain, &cols as &dyn NumericRead));
        for k in [1, 5, 50] {
            let expected = bm25::top_k_exhaustive_chained(
                &body,
                &terms,
                &stats,
                Bm25Params::default(),
                k,
                ctx,
            );
            let sig = |hits: &[bm25::ScoredDoc]| {
                hits.iter()
                    .map(|h| (h.doc_id, h.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            for floor in [f64::NEG_INFINITY, expected.last().unwrap().score] {
                let actual = bm25::top_k_pruned_chained(
                    &body,
                    &terms,
                    &stats,
                    Bm25Params::default(),
                    k,
                    floor,
                    ctx,
                );
                assert_eq!(sig(&actual), sig(&expected));
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}
fn value(row: usize) -> Option<u64> {
    if row < 16 {
        VALUES[row % 8]
    } else {
        None
    }
}
fn request(stages: Vec<pb::ScoreStage>) -> pb::Bm25SearchRequest {
    pb::Bm25SearchRequest {
        text: "word".into(),
        k: 32,
        analysis: Some(body_spec()),
        score_stages: stages,
        explain: true,
        projections: vec![pb::NamedProjection {
            name: "row".into(),
            expression: "row".into(),
        }],
        ..Default::default()
    }
}
fn check(hits: &[pb::Bm25Hit], stages: &[pb::ScoreStage]) -> Vec<(i64, u32)> {
    let mut sig = Vec::new();
    for hit in hits {
        let Some(pb::projected_value::Value::IntValue(row)) = hit.projected[0].value else {
            panic!("row missing")
        };
        let explain = hit.explain.as_ref().unwrap();
        let mut score = explain.bm25;
        assert_eq!(explain.stages.len(), stages.len());
        for (actual, stage) in explain.stages.iter().zip(stages) {
            let v = value(row as usize);
            assert_eq!(actual.present, v.is_some());
            if let Some(v) = v {
                assert_eq!(actual.input.to_bits(), input(v).to_bits());
                let effect = contribution(stage, v);
                // Proto3 default-valued doubles omit either signed zero on the wire.
                if effect == 0.0 {
                    assert_eq!(actual.contribution, 0.0);
                } else {
                    assert_eq!(actual.contribution.to_bits(), effect.to_bits());
                }
                score = if stage.operation
                    == Some(pb::score_stage::Operation::Op(
                        pb::ScoreOp::AddLinear as i32,
                    )) {
                    score + effect
                } else {
                    score * effect
                };
            }
            assert_eq!(actual.output.to_bits(), score.to_bits());
        }
        assert_eq!(hit.score.to_bits(), (score as f32).to_bits());
        sig.push((row, hit.score.to_bits()));
    }
    sig.sort_unstable();
    sig
}
async fn verify(addresses: Vec<String>, mono: String) {
    let mono = CoordinatorServiceImpl::new(vec![mono])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let (a, _, ah) = start_relay(addresses[..2].to_vec()).await;
    let (b, _, bh) = start_relay(addresses[2..].to_vec()).await;
    let (root, _, rh) = start_relay(vec![a.clone(), b.clone()]).await;
    // FetchValues is the path used by stored-value scorer dimensions.
    for (shard, addr) in addresses.iter().enumerate() {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let empty = client
            .bm25_query(pb::Bm25QueryRequest {
                score_stages: stages(),
                k: 0,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(empty.hits.is_empty());
        assert_eq!(empty.stage_columns_known, vec![shard < 3; 3]);
        let r = client
            .fetch_values(pb::FetchValuesRequest {
                candidate_ids: (shard as u64 * 8..shard as u64 * 8 + 8).collect(),
                stages: stages(),
                projections: vec![pb::CompiledProjection {
                    name: "row".into(),
                    expr: Some(pipestream_search::cel::compile_value("row").unwrap()),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.stage_columns_known, vec![shard < 3; 3]);
        assert_eq!(r.rows.len(), 8);
        for row in r.rows {
            let Some(pb::projected_value::Value::IntValue(id)) = row.values[0].value else {
                panic!("row missing")
            };
            for (actual, stage) in row.stage_values.iter().zip(stages()) {
                let expected = value(id as usize)
                    .map(|v| pb::projected_value::Value::DoubleValue(contribution(&stage, v)));
                assert_eq!(actual.value, expected);
            }
        }
    }
    for requested in [
        vec![],
        vec![stages()[0].clone()],
        vec![stages()[1].clone()],
        vec![stages()[2].clone()],
        stages(),
    ] {
        let expected = mono
            .bm25_search(Request::new(request(requested.clone())))
            .await
            .unwrap()
            .into_inner();
        let expected = check(&expected.hits, &requested);
        assert_eq!(expected.len(), 32);
        for roots in [
            addresses.clone(),
            vec![a.clone(), b.clone()],
            vec![root.clone()],
        ] {
            for stream in [false, true] {
                let coord = CoordinatorServiceImpl::new(roots.clone())
                    .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
                    .with_bm25_stream(stream);
                let result = coord
                    .bm25_search(Request::new(request(requested.clone())))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(check(&result.hits, &requested), expected);
            }
        }
    }
    for h in [ah, bh, rh] {
        h.abort();
        let _ = h.await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsigned_scores_explanations_and_signals_survive_relays_and_compaction() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 92171));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("uint_scores_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut configs = vec![];
        let mut addresses = vec![];
        let mut handles = vec![];
        for shard in 0..5 {
            let config = NodeConfig {
                index_path: Some(dir.join(format!("{shard}.tv"))),
                layout,
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: if shard < 4 { shard * 8 } else { 0 },
                integer_fields: vec!["row".into()],
                unsigned_integer_fields: if shard == 3 {
                    vec![]
                } else {
                    vec!["counter".into()]
                },
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                ..Default::default()
            };
            let (addr, h) = common::start_empty_node(config.clone()).await;
            let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            client
                .set_calibration(pb::SetCalibrationRequest {
                    dim: 8,
                    bit_width: 4,
                    shift: shift.clone(),
                    scale: scale.clone(),
                })
                .await
                .unwrap();
            let rows = if shard == 4 {
                0..32
            } else {
                shard as usize * 8..shard as usize * 8 + 8
            };
            for row in rows {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: vec!["word"; 1 + row % 3].join(" "),
                        analysis: Some(body_spec()),
                        integers: vec![pb::IntegerValue {
                            field: "row".into(),
                            value: row as i64,
                        }],
                        unsigned_integers: value(row)
                            .map(|v| pb::UnsignedIntegerValue {
                                field: "counter".into(),
                                value: v,
                            })
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 412 + row as u64),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            configs.push(config);
            addresses.push(addr);
            handles.push(h);
        }
        let mono = addresses.pop().unwrap();
        verify(addresses, mono).await;
        for h in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
        for round in 0..2 {
            let mut addresses = vec![];
            for (shard, config) in configs.iter().enumerate() {
                let (addr, h) = common::start_opened_node(config.clone()).await;
                if round == 0 {
                    NodeServiceClient::connect(addr.clone())
                        .await
                        .unwrap()
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: dir.join(format!("compact-{shard}")).display().to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(addr);
                handles.push(h);
            }
            let mono = addresses.pop().unwrap();
            verify(addresses, mono).await;
            for h in handles.drain(..) {
                h.abort();
                let _ = h.await;
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
