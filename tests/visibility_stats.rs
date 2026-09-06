mod common;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::*;
use tonic::transport::Channel;

const ROWS: [(&str, &str, &str); 4] = [
    ("alpha alpha", "red red", "public"),
    ("alpha secret secret secret", "private private", "private"),
    ("", "red", "public"),
    ("", "", "public"),
];

fn request(view: Option<WireVisibility>) -> TermStatsRequest {
    TermStatsRequest {
        terms: vec!["alpha".into(), "secret".into()],
        fields: vec![FieldTerms {
            field: "title".into(),
            terms: vec!["red".into(), "private".into()],
        }],
        visibility: view.map(|view| DocumentVisibility {
            filter: view.filter,
        }),
    }
}

fn same_statistics(actual: &TermStatsResponse, expected: &TermStatsResponse) {
    assert_eq!(actual.doc_count, expected.doc_count);
    assert_eq!(actual.total_doc_length, expected.total_doc_length);
    assert_eq!(actual.doc_frequencies, expected.doc_frequencies);
    assert_eq!(actual.field_stats, expected.field_stats);
}

// Keep the wire probe independent of generated request fields: this also
// represents a new planner talking to an older node that drops unknown fields.
#[derive(Clone, prost::Message)]
struct WireVisibility {
    #[prost(message, optional, tag = "1")]
    filter: Option<FilterExpr>,
}

#[derive(Clone, prost::Message)]
struct WireStatsRequest {
    #[prost(string, repeated, tag = "1")]
    terms: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    fields: Vec<FieldTerms>,
    #[prost(message, optional, tag = "3")]
    visibility: Option<WireVisibility>,
}

fn visibility(audience: &str) -> WireVisibility {
    WireVisibility {
        filter: Some(FilterExpr {
            expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                column: "audience".into(),
                values: vec![audience.into()],
            })),
        }),
    }
}

async fn stats(addr: &str, visibility: Option<WireVisibility>) -> TermStatsResponse {
    let channel = Channel::from_shared(addr.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    client
        .unary(
            tonic::Request::new(WireStatsRequest {
                terms: vec!["alpha".into(), "secret".into()],
                fields: vec![FieldTerms {
                    field: "title".into(),
                    terms: vec!["red".into(), "private".into()],
                }],
                visibility,
            }),
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/ai.protomolt.search.v1.NodeService/TermStats",
            ),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .unwrap()
        .into_inner()
}

async fn ingest(addr: &str, rows: &[(&str, &str, &str)]) {
    let mut client = NodeServiceClient::connect(addr.to_owned()).await.unwrap();
    client
        .add_documents(tokio_stream::iter(
            rows.iter()
                .map(|(body, title, audience)| AddDocumentsRequest {
                    text: if body.is_empty() { " " } else { body }.to_string(),
                    analysis: Some(body_spec()),
                    fields: (!title.is_empty())
                        .then(|| DocumentField {
                            field: "title".into(),
                            text: (*title).into(),
                            analysis: Some(body_spec()),
                        })
                        .into_iter()
                        .collect(),
                    facets: vec![FacetValue {
                        field: "audience".into(),
                        value: (*audience).into(),
                    }],
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        ))
        .await
        .unwrap();
}

fn config() -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        bm25_fields: vec!["body".into(), "title".into()],
        facet_fields: vec!["audience".into()],
        ..Default::default()
    }
}

#[tokio::test]
async fn visibility_statistics_equal_a_physically_restricted_corpus() {
    let (all, all_handle) = common::start_empty_node(config()).await;
    let (visible, visible_handle) = common::start_empty_node(config()).await;
    ingest(&all, &ROWS).await;
    ingest(&visible, &[ROWS[0], ROWS[2], ROWS[3]]).await;
    let scoped = stats(&all, Some(visibility("public"))).await;
    let reference = stats(&visible, None).await;
    assert_eq!(scoped.doc_count, reference.doc_count);
    assert_eq!(scoped.total_doc_length, reference.total_doc_length);
    assert_eq!(scoped.doc_frequencies, reference.doc_frequencies);
    assert_eq!(scoped.field_stats, reference.field_stats);
    assert_eq!(scoped.doc_count, 2);
    assert_eq!(scoped.doc_frequencies, vec![1, 0]);
    assert_eq!(scoped.field_stats[0].doc_frequencies, vec![2, 0]);
    let mut scores = Vec::new();
    for (addr, share, filter) in [
        (&all, &scoped, visibility("public").filter),
        (&visible, &reference, None),
    ] {
        let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
        let reply = client
            .bm25_query(Bm25QueryRequest {
                terms: vec!["alpha".into(), "secret".into()],
                global_doc_count: share.doc_count,
                global_total_doc_length: share.total_doc_length,
                global_doc_frequencies: share.doc_frequencies.clone(),
                expected_stats_epoch: share.stats_epoch,
                filter,
                k: 10,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        scores.push(
            reply
                .hits
                .iter()
                .map(|hit| hit.score.to_bits())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(scores[0], scores[1]);
    assert_eq!(scores[0].len(), 1);
    all_handle.abort();
    visible_handle.abort();
}

#[tokio::test]
async fn distinct_views_cannot_share_cache_entries_or_ignore_a_missing_echo() {
    use pipestream_search::stats_cache::StatsCache;
    use pipestream_search::visibility::VisibilityScope;
    let (addr, handle) = common::start_empty_node(config()).await;
    ingest(&addr, &ROWS).await;
    let cache = StatsCache::new(1);
    for audience in [None, Some("public"), Some("private"), Some("nobody")] {
        let req = request(audience.map(visibility));
        let scope = VisibilityScope::new(req.visibility.as_ref()).unwrap();
        assert!(cache.lookup_body_scoped(0, &req.terms, &scope).is_none());
        let reply = stats(&addr, audience.map(visibility)).await;
        scope.validate_response(&reply).unwrap();
        cache
            .store_scoped(0, &req.terms, &req.fields, &scope, &reply)
            .unwrap();
    }
    for (audience, count, dfs) in [
        (None, 3, vec![2, 1]),
        (Some("public"), 2, vec![1, 0]),
        (Some("private"), 1, vec![1, 1]),
        (Some("nobody"), 0, vec![0, 0]),
    ] {
        let req = request(audience.map(visibility));
        let scope = VisibilityScope::new(req.visibility.as_ref()).unwrap();
        let body = cache.lookup_body_scoped(0, &req.terms, &scope).unwrap();
        assert_eq!(body.doc_count, count);
        assert_eq!(body.dfs, dfs);
        let fused = cache.lookup_fused_scoped(0, &req.fields, &scope).unwrap();
        assert_eq!(fused.doc_count, count);
        let actual = stats(&addr, audience.map(visibility)).await;
        assert_eq!(fused.fields[0].dfs, actual.field_stats[0].doc_frequencies);
        assert_eq!(
            fused.visibility_columns_known,
            actual.visibility_columns_known
        );
    }
    let req = request(Some(visibility("public")));
    let scope = VisibilityScope::new(req.visibility.as_ref()).unwrap();
    let unrestricted = stats(&addr, None).await;
    assert!(cache
        .store_scoped(0, &req.terms, &req.fields, &scope, &unrestricted)
        .is_err());
    let scoped = stats(&addr, Some(visibility("public"))).await;
    assert!(cache.store(0, &req.terms, &req.fields, &scoped).is_err());
    // Refusing a mismatched response leaves the valid entry intact.
    assert_eq!(
        cache
            .lookup_body_scoped(0, &req.terms, &scope)
            .unwrap()
            .doc_count,
        2
    );
    ingest(&addr, &[("alpha", "red", "public")]).await;
    let changed = stats(&addr, None).await;
    cache.store(0, &req.terms, &req.fields, &changed).unwrap();
    assert!(cache.lookup_body_scoped(0, &req.terms, &scope).is_none());
    assert_eq!(cache.lookup_body(0, &req.terms).unwrap().doc_count, 4);
    handle.abort();
}

#[tokio::test]
async fn relay_levels_compose_the_same_visibility_and_reject_legacy_shares() {
    use pipestream_search::harness::start_relay;
    use pipestream_search::relay::merge_term_stats;
    let (a, ah) = common::start_empty_node(config()).await;
    let (b, bh) = common::start_empty_node(NodeConfig {
        slot_offset: 2,
        ..config()
    })
    .await;
    ingest(&a, &ROWS[..2]).await;
    ingest(&b, &ROWS[2..]).await;
    let (one, _, one_handle) = start_relay(vec![a.clone(), b.clone()]).await;
    let (two, _, two_handle) = start_relay(vec![one.clone()]).await;
    for audience in ["public", "private", "nobody"] {
        let req = request(Some(visibility(audience)));
        let children = vec![
            stats(&a, Some(visibility(audience))).await,
            stats(&b, Some(visibility(audience))).await,
        ];
        let flat = merge_term_stats(&req, &children).unwrap();
        for address in [&one, &two] {
            let relayed = stats(address, Some(visibility(audience))).await;
            same_statistics(&relayed, &flat);
            assert_eq!(relayed.visibility_fingerprint, flat.visibility_fingerprint);
            assert_eq!(relayed.visibility_columns_known, vec![true]);
            assert_ne!(relayed.stats_epoch, 0);
        }
        let mut legacy = children.clone();
        legacy[1].visibility_fingerprint.clear();
        assert!(merge_term_stats(&req, &legacy).is_err());
        let mut heterogeneous = children.clone();
        heterogeneous[1].visibility_columns_known[0] = false;
        assert_eq!(
            merge_term_stats(&req, &heterogeneous)
                .unwrap()
                .visibility_columns_known,
            vec![true]
        );
        let mut malformed = children;
        malformed[0].visibility_columns_known.clear();
        assert!(merge_term_stats(&req, &malformed).is_err());
    }
    for handle in [ah, bh, one_handle, two_handle] {
        handle.abort();
    }
}

#[tokio::test]
async fn empty_and_malformed_views_cannot_become_an_unrestricted_request() {
    use pipestream_search::visibility::VisibilityScope;
    let (addr, handle) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    for invalid in [
        DocumentVisibility::default(),
        DocumentVisibility {
            filter: Some(FilterExpr::default()),
        },
    ] {
        assert!(VisibilityScope::new(Some(&invalid)).is_err());
        let err = client
            .term_stats(TermStatsRequest {
                visibility: Some(invalid),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
    let req = request(Some(visibility("public")));
    let empty = client.term_stats(req.clone()).await.unwrap().into_inner();
    VisibilityScope::new(req.visibility.as_ref())
        .unwrap()
        .validate_response(&empty)
        .unwrap();
    assert_eq!(empty.doc_count, 0);
    assert_eq!(empty.visibility_columns_known, vec![false]);
    handle.abort();
}

#[tokio::test]
async fn visibility_survives_tombstones_flush_compaction_and_reopen() {
    use pipestream_search::node::Layout;
    const DIM: usize = 16;
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("visibility-{layout:?}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = NodeConfig {
            index_path: Some(dir.join("shard.tv")),
            layout,
            seal_tail_docs: 2,
            wal: true,
            ..config()
        };
        let vectors = common::unit_vectors(ROWS.len(), DIM, 9026);
        let (shift, scale) = common::fit_calibration(DIM, 4, &vectors);
        let (addr, handle) = common::start_empty_node(cfg.clone()).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        for (i, rows) in ROWS.chunks(2).enumerate() {
            ingest(&addr, rows).await;
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest {
                    vectors: vectors[i * 2 * DIM..(i + 1) * 2 * DIM].to_vec(),
                    dim: DIM as u32,
                }]))
                .await
                .unwrap();
        }
        client.flush(FlushRequest {}).await.unwrap();
        let initial = stats(&addr, Some(visibility("public"))).await;
        assert_eq!(initial.doc_count, 2);
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![0],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        let after_delete = stats(&addr, Some(visibility("public"))).await;
        assert_eq!(after_delete.doc_count, 1);
        assert_eq!(after_delete.total_doc_length, 0);
        assert_eq!(after_delete.doc_frequencies, vec![0, 0]);
        assert_eq!(after_delete.field_stats[0].total_doc_length, 1);
        assert_eq!(after_delete.field_stats[0].doc_frequencies, vec![1, 0]);
        assert!(after_delete.stats_epoch > initial.stats_epoch);
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![1],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        same_statistics(
            &stats(&addr, Some(visibility("public"))).await,
            &after_delete,
        );
        client.flush(FlushRequest {}).await.unwrap();
        same_statistics(
            &stats(&addr, Some(visibility("public"))).await,
            &after_delete,
        );
        let compacted = client
            .compact_shard(CompactShardRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(compacted.tombstones_reclaimed, 2);
        same_statistics(
            &stats(&addr, Some(visibility("public"))).await,
            &after_delete,
        );
        client.flush(FlushRequest {}).await.unwrap();
        drop(client);
        handle.abort();
        let _ = handle.await;
        let (reopened, handle) = common::start_opened_node(cfg).await;
        same_statistics(
            &stats(&reopened, Some(visibility("public"))).await,
            &after_delete,
        );
        handle.abort();
        let _ = handle.await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
