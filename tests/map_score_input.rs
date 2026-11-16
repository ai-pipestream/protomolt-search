mod common;

use pipestream_search::{
    node::{Bm25Shard, NodeConfig, NodeServiceImpl},
    pb::{self, node_service_server::NodeService},
    postings::{AnalyzedDoc, Bm25Store, ColumnKind::*},
};
fn map_stage(key: &str) -> pb::ScoreStage {
    pb::ScoreStage {
        op: pb::ScoreOp::AddLinear as i32,
        column: "metrics".into(),
        map_key: Some(key.into()),
        weight: 2.0,
        ..Default::default()
    }
}
fn store() -> Bm25Store {
    let mut store = Bm25Store::new().with_columns(MapNumeric, &["metrics"]);
    for row in 0..3 {
        store.add_document(
            row,
            "word".into(),
            AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
        );
    }
    store.set_map_numeric(0, 0, "", 10.0);
    store.set_map_numeric(0, 1, "", 0.0);
    store
}
#[tokio::test]
async fn explicit_map_score_input_preserves_empty_key_zero_and_absence() {
    let node = NodeServiceImpl::new(
        None,
        NodeConfig {
            map_numeric_fields: vec!["metrics".into()],
            ..Default::default()
        },
    )
    .with_bm25(Some(Bm25Shard::Building(store())));
    let response = node
        .fetch_values(tonic::Request::new(pb::FetchValuesRequest {
            candidate_ids: vec![0, 1, 2],
            stages: vec![map_stage("")],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.stage_columns_known, vec![true]);
    assert_eq!(
        response
            .rows
            .iter()
            .map(|row| row.stage_values[0].value.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(pb::projected_value::Value::DoubleValue(20.0)),
            Some(pb::projected_value::Value::DoubleValue(0.0)),
            None,
        ]
    );
}

#[tokio::test]
async fn invalid_map_score_operations_refuse_before_reading() {
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let mut cases = Vec::new();
    for op in [
        0,
        99,
        pb::ScoreOp::MultGeoDecayHaversine as i32,
        pb::ScoreOp::MultGeoDecayManhattan as i32,
    ] {
        let mut stage = map_stage("");
        stage.op = op;
        stage.scale = 1.0;
        cases.push(stage);
    }
    let mut nonfinite = map_stage("");
    nonfinite.weight = f64::INFINITY;
    cases.push(nonfinite);
    let mut negative_log = map_stage("");
    negative_log.op = pb::ScoreOp::MultLog as i32;
    negative_log.weight = -1.0;
    cases.push(negative_log);
    for stage in cases {
        let err = node
            .fetch_values(tonic::Request::new(pb::FetchValuesRequest {
                stages: vec![stage],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("score stage 0"));
    }
}

fn value(id: u64) -> Option<f64> {
    (!id.is_multiple_of(5)).then_some((id % 101) as f64 - 50.0)
}
fn effect(stage: &pb::ScoreStage, x: f64) -> (bool, f64) {
    assert!(stage.map_key.is_some(), "expected map stage");
    match pb::ScoreOp::try_from(stage.op).unwrap() {
        pb::ScoreOp::AddLinear => (true, stage.weight * x),
        pb::ScoreOp::MultLog => (false, 1.0 + stage.weight * (1.0 + x.max(0.0)).ln()),
        pb::ScoreOp::MultExpDecay => (false, (-((x - stage.origin).abs()) / stage.scale).exp()),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn map_score_bounds_explanations_and_fetches_agree_through_relays() {
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        coordinator::CoordinatorServiceImpl,
        harness::start_relay,
    };
    const ROWS: u64 = 1536;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-score-wire-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut handles = Vec::new();
    let mut addresses = Vec::new();
    for leaf in 0..2 {
        let mut data = Bm25Store::new().with_columns(MapNumeric, &["metrics"]);
        for row in 0..ROWS {
            data.add_document(
                row as u32,
                "word".into(),
                AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
            );
            if let Some(x) = value(leaf * ROWS + row) {
                data.set_map_numeric(0, row as u32, "", x);
            }
        }
        let shard = if leaf == 0 {
            Bm25Shard::Building(data)
        } else {
            let path = root.join("mapped.bm25");
            data.save(&path).unwrap();
            common::catalog::sealed_shard(
                &root.join("mapped.tv"),
                &path,
                Bm25Store::new().with_columns(MapNumeric, &["metrics"]),
            )
        };
        let node = NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: leaf * ROWS,
                map_numeric_fields: vec!["metrics".into()],
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                ..Default::default()
            },
        )
        .with_bm25(Some(shard));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(format!("http://{}", listener.local_addr().unwrap()));
        handles.push(tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        ));
    }
    let (relay, _, relay_handle) = start_relay(addresses.clone()).await;
    let (top, _, top_handle) = start_relay(vec![relay]).await;
    let mut chains = Vec::new();
    for (op, weight) in [
        (pb::ScoreOp::AddLinear, 2.0),
        (pb::ScoreOp::AddLinear, -2.0),
        (pb::ScoreOp::MultLog, 0.5),
        (pb::ScoreOp::MultExpDecay, 0.0),
    ] {
        chains.push(vec![pb::ScoreStage {
            op: op as i32,
            map_key: Some(String::new()),
            column: "metrics".into(),
            weight,
            scale: 10.0,
            ..Default::default()
        }]);
    }
    chains.push(vec![
        chains[0][0].clone(),
        chains[2][0].clone(),
        chains[3][0].clone(),
    ]);
    for children in [addresses.clone(), vec![top.clone()]] {
        let coordinator = CoordinatorServiceImpl::new(children)
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(coordinator.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let mut client = pb::search_service_client::SearchServiceClient::connect(address)
            .await
            .unwrap();
        let request = pb::Bm25SearchRequest {
            text: "word".into(),
            k: 7,
            analysis: Some(body_spec()),
            explain: true,
            ..Default::default()
        };
        let base = client
            .bm25_search(request.clone())
            .await
            .unwrap()
            .into_inner()
            .hits[0]
            .explain
            .as_ref()
            .unwrap()
            .bm25;
        for stages in &chains {
            let mut expected: Vec<_> = (0..2 * ROWS)
                .map(|id| {
                    let mut score = base;
                    if let Some(x) = value(id) {
                        for stage in stages {
                            let (add, amount) = effect(stage, x);
                            score = if add { score + amount } else { score * amount };
                        }
                    }
                    (id, score as f32)
                })
                .collect();
            expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            expected.truncate(7);
            let response = client
                .bm25_search(pb::Bm25SearchRequest {
                    score_stages: stages.clone(),
                    ..request.clone()
                })
                .await
                .unwrap()
                .into_inner();
            let actual: Vec<_> = response
                .hits
                .iter()
                .map(|hit| (hit.doc_id, hit.score.to_bits()))
                .collect();
            assert_eq!(
                actual,
                expected
                    .iter()
                    .map(|(id, score)| (*id, score.to_bits()))
                    .collect::<Vec<_>>()
            );
            for hit in response.hits {
                let explanation = hit.explain.as_ref().unwrap();
                for row in &explanation.stages {
                    assert_eq!(row.map_key.as_deref(), Some(""));
                    assert_eq!(row.present, value(hit.doc_id).is_some());
                    if let Some(x) = value(hit.doc_id) {
                        assert_eq!(row.input, x);
                    }
                }
                let tree = pipestream_search::explain::lexical("lex", &hit, &[], &[]).unwrap();
                assert!(tree.details[0].description.contains("metrics[\"\"]"));
            }
        }
        let err = client
            .bm25_search(pb::Bm25SearchRequest {
                score_stages: vec![map_stage("absent")],
                ..request
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("absent"));
        drop(client);
        server.abort();
        let _ = server.await;
    }
    // Candidate-scoped signals use the same selector through the relay.
    let mut node_client = pb::node_service_client::NodeServiceClient::connect(top)
        .await
        .unwrap();
    let response = node_client
        .fetch_values(pb::FetchValuesRequest {
            candidate_ids: vec![0, 1, 151, ROWS + 1],
            stages: vec![map_stage("")],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.stage_columns_known, vec![true]);
    for row in response.rows {
        assert_eq!(
            row.stage_values[0].value,
            value(row.doc_id).map(|v| pb::projected_value::Value::DoubleValue(2.0 * v))
        );
    }
    drop(node_client);
    relay_handle.abort();
    top_handle.abort();
    let _ = relay_handle.await;
    let _ = top_handle.await;
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
    std::fs::remove_dir_all(root).unwrap();
}
