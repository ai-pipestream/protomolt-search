//! Distributed bounding-box and haversine/equirectangular radius filter exactness.

use super::*;

/// Distributed geo filters: exact over a heterogeneous fleet where one
/// shard has no geo column at all, with every boundary rule the wire
/// promises pinned, and bitwise equal to the same corpus in one shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_geo_filters_are_exact_and_boundary_correct() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // Unfiltered, "rust" matches seven documents across all three
    // shards — including the two on the geo-less shard 2.
    let unfiltered = coordinator.fanout_bm25("rust", 10, None).await.unwrap();
    assert_eq!(ids(&unfiltered), vec![0, 1, 2, 3, 5, 6, 7]);

    // The unit box: d0 (0,0), d1 (0,1) and d2 (1,0) sit on three of its
    // corners, so this one assertion pins ALL FOUR edges as inclusive.
    // d3 and d6 are outside, d5 has no point, and d7 lives on a shard
    // with no geo column — the last two fail for the same reason, and
    // it is exact: no location is inside no region.
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "bbox edges are inclusive; absence and a missing column both fail"
    );
    // Shrinking each edge by a hair drops every corner it moved past,
    // which is what "inclusive" has to mean. d0 and d1 share latitude 0
    // and d0 and d2 share longitude 0, so the south and west edges take
    // two documents each and the north and east edges take one.
    for (case, want, f) in [
        (
            "south",
            vec![2u64],
            bbox_filter("courthouse", 1e-9, 1.0, 0.0, 1.0),
        ),
        (
            "north",
            vec![0, 1],
            bbox_filter("courthouse", 0.0, 1.0 - 1e-9, 0.0, 1.0),
        ),
        (
            "west",
            vec![1],
            bbox_filter("courthouse", 0.0, 1.0, 1e-9, 1.0),
        ),
        (
            "east",
            vec![0, 2],
            bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0 - 1e-9),
        ),
    ] {
        let (hits, _, _) = common::fanout::bm25_geo(&coordinator, "rust", 10, &[f], None)
            .await
            .unwrap();
        assert_eq!(ids(&hits), want, "{case} edge moved past its corner");
    }

    // Radius, both metrics: `distance <= meters` is inside. At the
    // equator one degree of longitude is one degree of latitude, so d1
    // and d2 are BOTH exactly at the boundary.
    let one_degree = geo::haversine_meters(0.0, 0.0, 0.0, 1.0);
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[radius_filter(
            "courthouse",
            0.0,
            0.0,
            one_degree,
            GeoMetric::Haversine,
        )],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "distance exactly == meters is in"
    );
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[radius_filter(
            "courthouse",
            0.0,
            0.0,
            one_degree.next_down(),
            GeoMetric::Haversine,
        )],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0],
        "one ULP tighter and the boundary drops"
    );
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[radius_filter(
            "courthouse",
            0.0,
            0.0,
            geo::M_PER_DEG_LAT,
            GeoMetric::Manhattan,
        )],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&hits),
        vec![0, 1, 2],
        "Manhattan pins the same boundary"
    );

    // The pole, where cos(lat) is zero: a box whose northern edge IS 90
    // contains it. ("vector" matches d1 and d4.)
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "vector",
        10,
        &[bbox_filter("courthouse", 89.0, 90.0, -180.0, 180.0)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![4], "lat exactly 90 is inside the box");

    // Just west of the antimeridian, reached by an ORDINARY box whose
    // eastern edge is 180. The wraparound box that would also reach it
    // is refused (see the refusal matrix).
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[bbox_filter("courthouse", -46.0, -44.0, 179.0, 180.0)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![6], "lon 179.9 needs no wraparound");

    // ALL filters must pass: the intersection, not the union.
    let (hits, _, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[
            bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0),
            bbox_filter("courthouse", 0.5, 90.0, -180.0, 180.0),
        ],
        None,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![2], "AND semantics, not OR");

    // Distributed == monolith, bitwise, with a filter applied.
    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;
    // The monolith declares the column, so the two documents that live
    // on the geo-less shard 2 simply carry no point there either --
    // "absent" and "column absent" have to agree, and this is the test
    // that they do.
    let all: Vec<(&str, Points)> = SHARD_DOCS.concat();
    add_documents_geo(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());
    for text in ["rust", "search rust", "vector"] {
        for filters in [
            vec![bbox_filter("courthouse", -90.0, 90.0, -180.0, 180.0)],
            vec![bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            vec![radius_filter(
                "courthouse",
                0.0,
                0.0,
                2_000_000.0,
                GeoMetric::Haversine,
            )],
        ] {
            let (got, _, _) = common::fanout::bm25_geo(&coordinator, text, 10, &filters, None)
                .await
                .unwrap();
            let (want, _, _) = common::fanout::bm25_geo(&monolithic, text, 10, &filters, None)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&want),
                "text {text:?}: distributed filtered hits must be bitwise equal"
            );
        }
    }

    // Facet counting is NOT narrowed by geo filters in this increment:
    // counts are over the match set as already defined, and the doc
    // says so loudly. Pinned here so a future CEL layer changing it is
    // a deliberate, visible change.
    let (hits, facets, _) = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![0, 1, 2]);
    assert!(facets.is_empty(), "none were requested");

    // The fused route carries filters too, forwarded like range facets.
    let fields = [QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    }];
    let (hits, _, _) = common::fanout::bm25_fused_req(
        &coordinator,
        pipestream_search::pb::Bm25SearchRequest {
            text: "rust".into(),
            fields: fields.to_vec(),
            geo_filters: vec![bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            ..Default::default()
        },
        10,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![0, 1, 2], "the fused route filters too");

    // The public RPC carries filters end to end.
    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            explain: false,
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            text: "rust".to_string(),
            k: 10,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: Vec::new(),
            map_facet_fields: Vec::new(),
            range_facet_fields: Vec::new(),
            geo_filters: vec![bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
            synonyms: Vec::new(),
            synonyms_off: false,
            pre_analyzed_terms: Vec::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(ids(&resp.hits), vec![0, 1, 2]);

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}
