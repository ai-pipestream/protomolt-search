//! Distributed geo distance-decay score-stage equivalence against a monolithic shard.

use super::*;

/// Distance-decay stages, distributed: bitwise equal to the monolith,
/// with absence as exact identity on the shard that has no geo column,
/// and a chain that actually reorders (otherwise the test proves
/// nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_geo_decay_matches_monolith() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, Points)> = SHARD_DOCS.concat();
    add_documents_geo(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    for op in [
        ScoreOp::MultGeoDecayHaversine,
        ScoreOp::MultGeoDecayManhattan,
    ] {
        // Origin at (1, 0): d2 sits ON it (factor 1), d0 and d1 are one
        // degree away, d3 and d6 are far.
        let stages = [geo_stage("courthouse", op, 1.0, 0.0, 100_000.0)];
        for text in ["rust", "search rust", "vector"] {
            let (got, _, _) = common::fanout::bm25_stages(&distributed, text, 10, &stages, None)
                .await
                .unwrap();
            let (want, _, _) = common::fanout::bm25_stages(&monolithic, text, 10, &stages, None)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&want),
                "{op:?} text {text:?}: distributed != monolith"
            );
        }

        // The chain must actually do something: the same match set, and
        // d2 (distance 0, factor 1) climbing over documents that beat it
        // unchained.
        let unchained = distributed.fanout_bm25("rust", 10, None).await.unwrap();
        let (chained, _, _) = common::fanout::bm25_stages(&distributed, "rust", 10, &stages, None)
            .await
            .unwrap();
        assert_eq!(ids(&unchained), ids(&chained), "a decay removes nothing");
        assert_eq!(chained[0].doc_id, 2, "{op:?}: the doc at the origin wins");
        // Documents with no point (d5) and documents on the shard with
        // no column (d7) pass through the stage unchanged — exact
        // identity, not degradation.
        for id in [5u64, 7] {
            let before = unchained.iter().find(|h| h.doc_id == id).unwrap().score;
            let after = chained.iter().find(|h| h.doc_id == id).unwrap().score;
            assert_eq!(
                before.to_bits(),
                after.to_bits(),
                "{op:?}: doc {id} has no point, so the stage is identity"
            );
        }
        // And a document WITH a point away from the origin really is
        // scaled down.
        let before = unchained.iter().find(|h| h.doc_id == 3).unwrap().score;
        let after = chained.iter().find(|h| h.doc_id == 3).unwrap().score;
        assert!(after < before, "{op:?}: doc 3 is far from the origin");
    }

    // Stage refusals, each naming the stage.
    for (bad, needle) in [
        (
            geo_stage("courthouse", ScoreOp::MultGeoDecayHaversine, 0.0, 0.0, 0.0),
            "scale > 0",
        ),
        (
            geo_stage(
                "courthouse",
                ScoreOp::MultGeoDecayHaversine,
                91.0,
                0.0,
                10.0,
            ),
            "[-90, 90]",
        ),
        (
            geo_stage(
                "courthouse",
                ScoreOp::MultGeoDecayManhattan,
                0.0,
                f64::NAN,
                10.0,
            ),
            "[-180, 180]",
        ),
    ] {
        let err = common::fanout::bm25_stages(&distributed, "rust", 10, &[bad], None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // A geo column NO shard knows is refused by the chain's typo rule.
    let err = common::fanout::bm25_stages(
        &distributed,
        "rust",
        10,
        &[geo_stage(
            "courthosue",
            ScoreOp::MultGeoDecayHaversine,
            0.0,
            0.0,
            10.0,
        )],
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("courthosue"), "{}", err.message());

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}
