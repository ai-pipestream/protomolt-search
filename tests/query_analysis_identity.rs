//! Analyzer identity must survive each route that uses analyzed terms, including
//! an absent optional column and a relay. Identical tokens under different specs
//! deliberately exercise the contract rather than an accidental no-match result.
use pipestream_search::{
    analyzer::{analysis_fingerprint, body_spec, cased_body_spec},
    coordinator::CoordinatorServiceImpl,
    harness::{fit_calibration, start_empty_node, start_relay, unit_vectors},
    node::NodeConfig,
    pb::{node_service_client::NodeServiceClient, search_service_server::SearchService, *},
    sha256::{to_hex, Sha256},
};
use prost::Message;
use tonic::{Code, Request, Status};

const DIM: usize = 8;
const DOCS: u32 = 3;

struct Fixture {
    addr: String,
    vector: Vec<f32>,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.handles {
            task.abort();
        }
    }
}
impl Fixture {
    async fn new(explicit: bool, populated: bool) -> Self {
        Self::new_at(explicit, populated, None).await
    }
    async fn new_at(
        explicit: bool,
        populated: bool,
        index_path: Option<std::path::PathBuf>,
    ) -> Self {
        let (addr, task) = start_empty_node(NodeConfig {
            analysis_addr: Some("native".into()),
            bm25_fields: vec!["body".into(), "caption".into()],
            integer_fields: vec!["ordinal".into()],
            index_path,
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let vectors = unit_vectors(DOCS as usize, DIM, 0xABCDEF);
        let (shift, scale) = fit_calibration(DIM, 4, &vectors);
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        if explicit {
            let bytes = MappedAnalysisContract {
                fields: vec![
                    MappedAnalysisColumn {
                        path: "body".into(),
                        name: "body".into(),
                        analysis: Some(body_spec()),
                    },
                    MappedAnalysisColumn {
                        path: "caption".into(),
                        name: "caption".into(),
                        analysis: Some(cased_body_spec()),
                    },
                ],
            }
            .encode_to_vec();
            let mut hash = Sha256::new();
            hash.update(b"protomolt.search.mapped-analysis.v1\0");
            hash.update(&bytes);
            client
                .apply_wal_binding(ApplyWalBindingRequest {
                    plan_fingerprint: "query-analysis-fixture".into(),
                    body_path: "body".into(),
                    analysis_sha: to_hex(&hash.finalize()),
                    analysis_contract: bytes,
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        if populated {
            client
                .add_documents(tokio_stream::iter((0..DOCS).map(|id| {
                    AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        fields: vec![DocumentField {
                            field: "caption".into(),
                            text: "word".into(),
                            analysis: Some(cased_body_spec()),
                        }],
                        integers: vec![IntegerValue {
                            field: "ordinal".into(),
                            value: i64::from(id),
                        }],
                        ..Default::default()
                    }
                })))
                .await
                .unwrap();
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest {
                    dim: DIM as u32,
                    vectors: vectors.clone(),
                }]))
                .await
                .unwrap();
        }
        Self {
            addr,
            vector: vectors[..DIM].to_vec(),
            handles: vec![task],
        }
    }
    async fn client(&self) -> NodeServiceClient<tonic::transport::Channel> {
        NodeServiceClient::connect(self.addr.clone()).await.unwrap()
    }
    fn coordinator(&self, stream: bool) -> CoordinatorServiceImpl {
        CoordinatorServiceImpl::new(vec![self.addr.clone()])
            .with_bm25(Some("native".into()), Default::default())
            .with_bm25_stream(stream)
    }
    async fn add_relay(&mut self) {
        let (addr, _, task) = start_relay(vec![self.addr.clone()]).await;
        self.addr = addr;
        self.handles.push(task);
    }
}

fn flat(fp: u64) -> Bm25QueryRequest {
    Bm25QueryRequest {
        terms: vec!["word".into()],
        k: DOCS,
        global_doc_count: DOCS.into(),
        global_total_doc_length: DOCS.into(),
        global_doc_frequencies: vec![DOCS],
        analysis_fingerprint: fp,
        ..Default::default()
    }
}
fn fused(field: &str, fp: u64) -> Bm25QueryRequest {
    Bm25QueryRequest {
        k: DOCS,
        global_doc_count: DOCS.into(),
        fields: vec![Bm25FieldLeg {
            field: field.into(),
            terms: vec!["word".into()],
            global_total_doc_length: DOCS.into(),
            global_doc_frequencies: vec![DOCS],
            weight: 1.0,
            analysis_fingerprint: fp,
            ..Default::default()
        }],
        ..Default::default()
    }
}
fn expect<T: std::fmt::Debug>(result: Result<T, Status>, valid: bool, route: &str) {
    if valid {
        assert!(result.is_ok(), "{route}: {result:?}");
    } else {
        let error = result.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition, "{route}: {error}");
        assert!(
            error.message().contains("analyzer fingerprint")
                || error
                    .message()
                    .contains("native analysis requires an explicit AnalysisSpec"),
            "{route}: {error}"
        );
    }
}

#[tokio::test]
async fn every_internal_lexical_route_checks_the_indexed_analysis() {
    let good = analysis_fingerprint(Some(&body_spec()));
    let wrong = analysis_fingerprint(Some(&cased_body_spec()));
    assert_ne!(good, wrong);
    for explicit in [false, true] {
        let fixture = Fixture::new(explicit, true).await;
        let mut client = fixture.client().await;
        for fp in [good, wrong, 0] {
            let valid = fp == good || (fp == 0 && !explicit);
            expect(client.bm25_query(flat(fp)).await, valid, "flat");
            expect(
                client.bm25_query(fused("body", fp)).await,
                valid,
                "fused body",
            );
            expect(
                client
                    .bm25_rescore(Bm25RescoreRequest {
                        terms: vec!["word".into()],
                        global_doc_count: DOCS.into(),
                        global_total_doc_length: DOCS.into(),
                        global_doc_frequencies: vec![DOCS],
                        candidate_ids: vec![0, 1, 2],
                        analysis_fingerprint: fp,
                        ..Default::default()
                    })
                    .await,
                valid,
                "rescore",
            );
            expect(
                client
                    .hybrid_shard(HybridShardRequest {
                        k: DOCS,
                        vector: fixture.vector.clone(),
                        terms: vec!["word".into()],
                        global_doc_count: DOCS.into(),
                        global_total_doc_length: DOCS.into(),
                        global_doc_frequencies: vec![DOCS],
                        analysis_fingerprint: fp,
                        ..Default::default()
                    })
                    .await,
                valid,
                "hybrid shard",
            );
            expect(
                client
                    .shard_legs(ShardLegsRequest {
                        k: DOCS,
                        vector: fixture.vector.clone(),
                        terms: vec!["word".into()],
                        global_doc_count: DOCS.into(),
                        global_total_doc_length: DOCS.into(),
                        global_doc_frequencies: vec![DOCS],
                        analysis_fingerprint: fp,
                        ..Default::default()
                    })
                    .await,
                valid,
                "shard legs",
            );
            expect(
                client
                    .resolve_lexical_bitmap(LexicalBitmapRequest {
                        terms: vec!["word".into()],
                        analysis_fingerprint: fp,
                    })
                    .await,
                valid,
                "membership",
            );
            expect(
                client
                    .browse_shard(BrowseShardRequest {
                        k: DOCS,
                        first_page: true,
                        lexical_terms: vec!["word".into()],
                        analysis_fingerprint: fp,
                        ..Default::default()
                    })
                    .await,
                valid,
                "lexical browse",
            );
            // Facets/projections can use k=0, so the check must precede scoring.
            let mut zero = flat(fp);
            zero.k = 0;
            expect(client.bm25_query(zero).await, valid, "flat k=0");
            let mut zero = fused("body", fp);
            zero.k = 0;
            expect(client.bm25_query(zero).await, valid, "fused k=0");
        }
        // A different column has a different contract, even for the same token.
        expect(
            client.bm25_query(fused("caption", wrong)).await,
            true,
            "caption own spec",
        );
        expect(
            client.bm25_query(fused("caption", good)).await,
            false,
            "caption body spec",
        );
        expect(
            client.bm25_query(fused("caption", 0)).await,
            !explicit,
            "caption unknown spec",
        );
        client
            .browse_shard(BrowseShardRequest {
                k: DOCS,
                first_page: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn explicit_binding_is_checked_before_any_rows_exist() {
    let fixture = Fixture::new(true, false).await;
    let mut client = fixture.client().await;
    for fp in [0, analysis_fingerprint(Some(&cased_body_spec()))] {
        expect(client.bm25_query(flat(fp)).await, false, "empty bound body");
    }
    expect(
        client
            .bm25_query(fused("caption", analysis_fingerprint(Some(&body_spec()))))
            .await,
        false,
        "empty bound caption",
    );
    expect(
        client
            .bm25_query(flat(analysis_fingerprint(Some(&body_spec()))))
            .await,
        true,
        "empty matching body",
    );
}

fn lexical(spec: Option<AnalysisSpec>) -> SearchQuery {
    SearchQuery {
        id: "lex".into(),
        query: Some(search_query::Query::Lexical(LexicalQuery {
            text: "word".into(),
            analysis: spec,
            ..Default::default()
        })),
    }
}
fn selection(search: SearchQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(search)),
    }
}

#[tokio::test]
async fn public_queries_carry_identity_through_scoring_sorting_boosts_and_membership() {
    let fixture = Fixture::new(true, true).await;
    for stream in [false, true] {
        let coord = fixture.coordinator(stream);
        // Reuse one coordinator so a cached stats result cannot mask a mismatch.
        for spec in [
            Some(body_spec()),
            Some(cased_body_spec()),
            None,
            Some(body_spec()),
        ] {
            let valid = spec == Some(body_spec());
            let response = coord
                .bm25_search(Request::new(Bm25SearchRequest {
                    text: "word".into(),
                    k: DOCS,
                    analysis: spec.clone(),
                    ..Default::default()
                }))
                .await;
            if valid {
                assert_eq!(
                    response.as_ref().unwrap().get_ref().hits.len(),
                    DOCS as usize
                );
            }
            expect(response, valid, "public BM25");
            let response = coord
                .bm25_search(Request::new(Bm25SearchRequest {
                    text: "word".into(),
                    k: DOCS,
                    fields: vec![QueryField {
                        field: "body".into(),
                        analysis: spec.clone(),
                        weight: 1.0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .await;
            expect(response, valid, "public fused BM25");
            for sorted in [false, true] {
                let response = coord
                    .query(Request::new(QueryRequest {
                        k: DOCS,
                        selection: Some(selection(lexical(spec.clone()))),
                        sort: if sorted {
                            vec![QuerySort {
                                column: "ordinal".into(),
                                descending: true,
                            }]
                        } else {
                            vec![]
                        },
                        ..Default::default()
                    }))
                    .await;
                if valid {
                    assert_eq!(
                        response.as_ref().unwrap().get_ref().hits.len(),
                        DOCS as usize
                    );
                }
                expect(response, valid, "public lexical query/sort");
            }
            let response = coord
                .query(Request::new(QueryRequest {
                    k: DOCS,
                    selection: Some(SelectionQuery {
                        node: Some(selection_query::Node::Boolean(BooleanQuery {
                            must: vec![selection(lexical(spec.clone()))],
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                }))
                .await;
            if valid {
                assert_eq!(
                    response.as_ref().unwrap().get_ref().hits.len(),
                    DOCS as usize
                );
            }
            expect(response, valid, "Boolean membership and candidate scoring");
            let response = coord
                .query(Request::new(QueryRequest {
                    k: DOCS,
                    selection: Some(selection(SearchQuery {
                        id: "dense".into(),
                        query: Some(search_query::Query::Dense(DenseQuery {
                            vector: fixture.vector.clone(),
                            ..Default::default()
                        })),
                    })),
                    boosts: vec![BoostQuery {
                        query: Some(lexical(spec.clone())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .await;
            expect(response, valid, "candidate lexical boost");
            for mode in [
                FusionMode::GlobalRank,
                FusionMode::TwoLevel,
                FusionMode::Cascade,
                FusionMode::ScoreBlend,
                FusionMode::Decomposed,
            ] {
                let response = coord
                    .hybrid_search(Request::new(HybridSearchRequest {
                        text: "word".into(),
                        k: DOCS,
                        vector: fixture.vector.clone(),
                        analysis: spec.clone(),
                        legs: Some(HybridLegOptions {
                            fusion_mode: mode as i32,
                            leg_k: DOCS,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }))
                    .await;
                expect(response, valid, &format!("hybrid {mode:?}"));
            }
        }
    }
}

#[tokio::test]
async fn two_relay_levels_preserve_identity_and_refusals() {
    let mut fixture = Fixture::new(true, true).await;
    for depth in 0..=2 {
        if depth != 0 {
            fixture.add_relay().await;
        }
        let mut client = fixture.client().await;
        for fp in [
            analysis_fingerprint(Some(&body_spec())),
            analysis_fingerprint(Some(&cased_body_spec())),
            0,
        ] {
            let valid = fp == analysis_fingerprint(Some(&body_spec()));
            expect(client.bm25_query(flat(fp)).await, valid, "relay flat");
            expect(
                client.bm25_query(fused("body", fp)).await,
                valid,
                "relay fused",
            );
        }
        for stream in [false, true] {
            let coord = fixture.coordinator(stream);
            for spec in [Some(body_spec()), Some(cased_body_spec()), None] {
                let valid = spec == Some(body_spec());
                let response = coord
                    .bm25_search(Request::new(Bm25SearchRequest {
                        text: "word".into(),
                        k: DOCS,
                        analysis: spec,
                        ..Default::default()
                    }))
                    .await;
                if valid {
                    assert_eq!(
                        response.as_ref().unwrap().get_ref().hits.len(),
                        DOCS as usize
                    );
                }
                expect(response, valid, "relay public/streaming BM25");
            }
        }
    }
}

#[tokio::test]
async fn absent_optional_field_keeps_its_query_contract_after_restart() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("query_analysis_restart_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("shard.tv");
    let mut fixture = Fixture::new_at(true, false, Some(path.clone())).await;
    let mut client = fixture.client().await;
    client
        .add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "word".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        }]))
        .await
        .unwrap();
    client
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            dim: DIM as u32,
            vectors: fixture.vector.clone(),
        }]))
        .await
        .unwrap();
    client.flush(FlushRequest {}).await.unwrap();
    for reopened in [false, true] {
        if reopened {
            drop(client);
            let task = fixture.handles.pop().unwrap();
            task.abort();
            let _ = task.await;
            let (addr, task) = pipestream_search::harness::start_opened_node(NodeConfig {
                analysis_addr: Some("native".into()),
                bm25_fields: vec!["body".into(), "caption".into()],
                integer_fields: vec!["ordinal".into()],
                index_path: Some(path.clone()),
                ..Default::default()
            })
            .await;
            fixture.addr = addr;
            fixture.handles.push(task);
            client = fixture.client().await;
        }
        let response = client
            .bm25_query(flat(analysis_fingerprint(Some(&body_spec()))))
            .await
            .unwrap();
        assert_eq!(response.get_ref().hits.len(), 1);
        let response = client
            .bm25_query(fused(
                "caption",
                analysis_fingerprint(Some(&cased_body_spec())),
            ))
            .await
            .unwrap();
        assert!(response.get_ref().hits.is_empty());
        for fp in [0, analysis_fingerprint(Some(&body_spec()))] {
            expect(
                client.bm25_query(fused("caption", fp)).await,
                false,
                "absent optional field",
            );
        }
        expect(
            client.bm25_query(flat(0)).await,
            false,
            "persisted explicit body",
        );
    }
    drop(client);
    drop(fixture);
    std::fs::remove_dir_all(dir).unwrap();
}
