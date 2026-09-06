use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    cel::{compile_filter, compile_value},
    link::NodeLink,
    node::{Layout, NodeConfig, NodeServiceImpl},
    pb::*,
    visibility::VisibilityScope,
};
use std::sync::Arc;
use tonic::Code;

fn view(predicate: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: compile_filter(predicate).unwrap(),
    }
}
fn aggregate(visibility: Option<DocumentVisibility>) -> AggregateShardRequest {
    AggregateShardRequest {
        visibility,
        aggregations: vec![CompiledAggregation {
            name: "count".into(),
            expr: Some(compile_value("1").unwrap()),
            op: AggregateOp::Count as i32,
            ..Default::default()
        }],
        percentiles: vec![CompiledPercentile {
            name: "n".into(),
            expr: Some(compile_value("n").unwrap()),
        }],
        ..Default::default()
    }
}
async fn append(link: &mut NodeLink, n: i64, audience: &str) {
    link.add_documents(tokio_stream::iter([AddDocumentsRequest {
        text: "alpha".into(),
        analysis: Some(body_spec()),
        integers: vec![IntegerValue {
            field: "n".into(),
            value: n,
        }],
        facets: vec![FacetValue {
            field: "audience".into(),
            value: audience.into(),
        }],
        ..Default::default()
    }]))
    .await
    .unwrap();
}
fn config(layout: Layout) -> NodeConfig {
    NodeConfig {
        layout,
        slot_offset: 100,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        integer_fields: vec!["n".into()],
        facet_fields: vec!["audience".into()],
        ..Default::default()
    }
}
#[tokio::test]
async fn browse_aggregate_and_quantile_rounds_share_the_view_and_physical_version() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let mut link = NodeLink::local(Arc::new(NodeServiceImpl::new(None, config(layout))));
        for (n, audience) in [
            (1, "public"),
            (100, "private"),
            (3, "public"),
            (200, "private"),
        ] {
            append(&mut link, n, audience).await;
        }
        for (predicate, expected) in [
            ("audience == 'public'", vec![100, 102]),
            ("audience == 'private'", vec![101, 103]),
            ("audience == 'nobody'", vec![]),
        ] {
            let visibility = Some(view(predicate));
            let scope = VisibilityScope::new(visibility.as_ref()).unwrap();
            let all = aggregate(visibility.clone());
            let phase = link
                .aggregate_shard(all.clone())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(phase.matched, expected.len() as u64);
            scope
                .validate_echo(
                    &phase.visibility_fingerprint,
                    &phase.visibility_columns_known,
                )
                .unwrap();
            assert_eq!(phase.visibility_columns_known, vec![true]);
            assert!(phase.stats_epoch > 0);
            assert_eq!(phase.stats_incarnation.len(), 32);
            let mut browse = BrowseShardRequest {
                visibility: visibility.clone(),
                k: 10,
                first_page: true,
                expected_stats_epoch: phase.stats_epoch,
                expected_stats_incarnation: phase.stats_incarnation.clone(),
                sort: vec![BrowseSort {
                    column: "n".into(),
                    descending: false,
                }],
                ..Default::default()
            };
            let mut round = QuantileCountsRequest {
                visibility: visibility.clone(),
                exprs: all.percentiles.clone(),
                targets: vec![QuantileTarget {
                    expr_index: 0,
                    threshold_bits: u64::MAX,
                }],
                expected_stats_epoch: phase.stats_epoch,
                expected_stats_incarnation: phase.stats_incarnation.clone(),
                ..Default::default()
            };
            for user in [
                "",
                "audience == 'public' || audience == 'private'",
                "audience == 'nobody'",
            ] {
                browse.filter = compile_filter(user).unwrap();
                round.filter = browse.filter.clone();
                let mut agg = all.clone();
                agg.filter = browse.filter.clone();
                let count = if user == "audience == 'nobody'" {
                    0
                } else {
                    expected.len() as u64
                };
                let b = link
                    .browse_shard(browse.clone())
                    .await
                    .unwrap()
                    .into_inner();
                let a = link.aggregate_shard(agg).await.unwrap().into_inner();
                let q = link
                    .quantile_counts(round.clone())
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    b.doc_ids,
                    if count == 0 { vec![] } else { expected.clone() }
                );
                assert_eq!(a.matched, count);
                assert_eq!(q.counts, vec![count]);
                for (epoch, incarnation, fp, known) in [
                    (
                        b.stats_epoch,
                        &b.stats_incarnation,
                        &b.visibility_fingerprint,
                        &b.visibility_columns_known,
                    ),
                    (
                        a.stats_epoch,
                        &a.stats_incarnation,
                        &a.visibility_fingerprint,
                        &a.visibility_columns_known,
                    ),
                    (
                        q.stats_epoch,
                        &q.stats_incarnation,
                        &q.visibility_fingerprint,
                        &q.visibility_columns_known,
                    ),
                ] {
                    assert_eq!(epoch, phase.stats_epoch);
                    assert_eq!(incarnation, &phase.stats_incarnation);
                    scope.validate_echo(fp, known).unwrap();
                }
            }
            // Candidate pools can only narrow the authority's document view.
            for ids in [vec![], vec![101, 103], vec![100, 101, 102, 103, 999]] {
                let wanted = expected.iter().filter(|id| ids.contains(id)).count() as u64;
                let mut agg = all.clone();
                agg.restrict_doc_ids = true;
                agg.doc_ids = ids.clone();
                round.filter = None;
                round.restrict_doc_ids = true;
                round.doc_ids = ids;
                assert_eq!(
                    link.aggregate_shard(agg).await.unwrap().get_ref().matched,
                    wanted
                );
                assert_eq!(
                    link.quantile_counts(round.clone())
                        .await
                        .unwrap()
                        .get_ref()
                        .counts,
                    vec![wanted]
                );
            }
        }
        let admitted = link
            .aggregate_shard(aggregate(None))
            .await
            .unwrap()
            .into_inner();
        append(&mut link, 5, "public").await;
        let mut stale = aggregate(None);
        stale.expected_stats_epoch = admitted.stats_epoch;
        stale.expected_stats_incarnation = admitted.stats_incarnation.clone();
        assert_eq!(
            link.aggregate_shard(stale).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            link.browse_shard(BrowseShardRequest {
                k: 10,
                first_page: true,
                expected_stats_epoch: admitted.stats_epoch,
                expected_stats_incarnation: admitted.stats_incarnation.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            link.quantile_counts(QuantileCountsRequest {
                expected_stats_epoch: admitted.stats_epoch,
                expected_stats_incarnation: admitted.stats_incarnation,
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
    }
}
#[tokio::test]
async fn empty_shards_still_validate_claims_and_echo_unknown_authority_columns() {
    let mut link = NodeLink::local(Arc::new(NodeServiceImpl::new(
        None,
        config(Layout::SingleImage),
    )));
    let visibility = Some(view("audience == 'public'"));
    let phase = link
        .aggregate_shard(aggregate(visibility.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(phase.matched, 0);
    assert_eq!(phase.visibility_columns_known, vec![false]);
    let scope = VisibilityScope::new(visibility.as_ref()).unwrap();
    scope
        .validate_echo(
            &phase.visibility_fingerprint,
            &phase.visibility_columns_known,
        )
        .unwrap();
    for (epoch, incarnation) in [
        (0, vec![]),
        (phase.stats_epoch, vec![]),
        (0, phase.stats_incarnation.clone()),
        (phase.stats_epoch, vec![1; 31]),
        (phase.stats_epoch, vec![1; 32]),
    ] {
        assert_eq!(
            link.quantile_counts(QuantileCountsRequest {
                expected_stats_epoch: epoch,
                expected_stats_incarnation: incarnation.clone(),
                visibility: visibility.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        if epoch != 0 || !incarnation.is_empty() {
            let mut input = aggregate(visibility.clone());
            input.expected_stats_epoch = epoch;
            input.expected_stats_incarnation = incarnation.clone();
            assert_eq!(
                link.aggregate_shard(input).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            assert_eq!(
                link.browse_shard(BrowseShardRequest {
                    k: 1,
                    first_page: true,
                    visibility: visibility.clone(),
                    expected_stats_epoch: epoch,
                    expected_stats_incarnation: incarnation,
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
                Code::FailedPrecondition
            );
        }
    }
    let q = link
        .quantile_counts(QuantileCountsRequest {
            visibility: visibility.clone(),
            expected_stats_epoch: phase.stats_epoch,
            expected_stats_incarnation: phase.stats_incarnation.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    scope
        .validate_echo(&q.visibility_fingerprint, &q.visibility_columns_known)
        .unwrap();
    assert_eq!(q.visibility_columns_known, vec![false]);
    let malformed = Some(DocumentVisibility::default());
    assert_eq!(
        link.aggregate_shard(aggregate(malformed.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        link.browse_shard(BrowseShardRequest {
            k: 1,
            first_page: true,
            visibility: malformed.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        link.quantile_counts(QuantileCountsRequest {
            visibility: malformed,
            expected_stats_epoch: phase.stats_epoch,
            expected_stats_incarnation: phase.stats_incarnation,
            ..Default::default()
        })
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
}
