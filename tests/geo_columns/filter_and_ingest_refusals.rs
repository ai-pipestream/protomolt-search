//! Loud refusal of malformed geo filters and invalid geo-point ingest coordinates.

use super::*;

/// Every geo-filter refusal, each naming the column and the knob. The
/// coordinator runs the shard-free half BEFORE its zero-term/k=0 early
/// return on both fan-out routes, so a malformed filter cannot hide
/// behind an empty Ok.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn geo_filter_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_geo_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // A column NO shard knows is a typo, not an empty result set.
    let err = common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[bbox_filter("courthosue", 0.0, 1.0, 0.0, 1.0)],
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("courthosue") && err.message().contains("--geo-fields"),
        "the refusal names the column and the knob: {}",
        err.message()
    );

    // A column only SOME shards know is the heterogeneous fleet, and is
    // fine — shard 2 has no geo column and contributes nothing.
    common::fanout::bm25_geo(
        &coordinator,
        "rust",
        10,
        &[bbox_filter("courthouse", 0.0, 1.0, 0.0, 1.0)],
        None,
    )
    .await
    .expect("a partially known column is exact, not an error");

    let antimeridian = GeoFilter {
        column: "courthouse".to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Bbox(GeoBbox {
            min_lat: -46.0,
            max_lat: -44.0,
            min_lon: 179.0,
            max_lon: -179.0,
        })),
    };
    let no_region = GeoFilter {
        column: "courthouse".to_string(),
        region: None,
    };
    let unspecified_metric = GeoFilter {
        column: "courthouse".to_string(),
        region: Some(pipestream_search::pb::geo_filter::Region::Radius(
            GeoRadius {
                lat: 0.0,
                lon: 0.0,
                meters: 1000.0,
                metric: 0,
            },
        )),
    };
    for (bad, needle) in [
        (antimeridian, "antimeridian"),
        (
            bbox_filter("courthouse", 1.0, 0.0, 0.0, 1.0),
            "above max_lat",
        ),
        (bbox_filter("courthouse", 0.0, 91.0, 0.0, 1.0), "[-90, 90]"),
        (
            bbox_filter("courthouse", 0.0, 1.0, 0.0, 181.0),
            "[-180, 180]",
        ),
        (
            bbox_filter("courthouse", f64::NAN, 1.0, 0.0, 1.0),
            "not a finite degree",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, 0.0, GeoMetric::Haversine),
            "above zero",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, -5.0, GeoMetric::Haversine),
            "above zero",
        ),
        (
            radius_filter("courthouse", 0.0, 0.0, f64::INFINITY, GeoMetric::Haversine),
            "finite",
        ),
        (no_region, "no region set"),
        (unspecified_metric, "unknown geo metric"),
        (bbox_filter("", 0.0, 1.0, 0.0, 1.0), "names the geo column"),
    ] {
        let err = common::fanout::bm25_geo(&coordinator, "rust", 10, &[bad], None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }

    // Filter validation needs no shard, so the coordinator's zero-term
    // and k = 0 early returns must not swallow it into an empty Ok --
    // on BOTH fan-out routes.
    let bad = bbox_filter("courthouse", 0.0, 1.0, 179.0, -179.0);
    let bad_one = std::slice::from_ref(&bad);
    for (text, k) in [("", 10u32), ("rust", 0)] {
        let err = common::fanout::bm25_geo(&coordinator, text, k, bad_one, None)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "text {text:?}, k {k}: the early return must not hide the refusal"
        );
    }
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
    let err = common::fanout::bm25_fused_req(
        &coordinator,
        pipestream_search::pb::Bm25SearchRequest {
            fields: fields.to_vec(),
            geo_filters: bad_one.to_vec(),
            ..Default::default()
        },
        10,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the fused route honors the same rule before its own early return"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The geo ingest refusal matrix: coordinates off the globe, non-finite
/// coordinates, an unknown field, and a repeat within one document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn geo_ingest_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["courthouse".to_string()],
        ..Default::default()
    })
    .await;

    for (points, needle) in [
        (
            &[("courthouse", 90.000_001, 0.0)][..],
            "latitude 90.000001 is not a finite degree in [-90, 90]",
        ),
        (&[("courthouse", -90.5, 0.0)][..], "[-90, 90]"),
        (&[("courthouse", 0.0, 180.5)][..], "[-180, 180]"),
        (&[("courthouse", f64::NAN, 0.0)][..], "[-90, 90]"),
        (&[("courthouse", 0.0, f64::INFINITY)][..], "[-180, 180]"),
        (
            &[("courthosue", 0.0, 0.0)][..],
            "unknown geo field \"courthosue\"",
        ),
        (
            &[("courthouse", 0.0, 0.0), ("courthouse", 1.0, 1.0)][..],
            "repeats in one document",
        ),
    ] {
        let err = add_documents_geo(&addr, &[("rust", points)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // The pole and the antimeridian ARE valid coordinates, and ingest
    // must not confuse "extreme" with "invalid".
    add_documents_geo(
        &addr,
        &[
            ("rust north", &[("courthouse", 90.0, 180.0)]),
            ("rust south", &[("courthouse", -90.0, -180.0)]),
            ("rust nowhere", &[]),
        ],
    )
    .await
    .expect("the corners of the coordinate domain are points, not errors");

    // The geo-field knob is named in the unknown-field refusal even on a
    // shard with no geo table at all.
    let (bare_addr, bare) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let err = add_documents_geo(&bare_addr, &[("rust", &[("courthouse", 0.0, 0.0)])])
        .await
        .unwrap_err();
    assert!(err.message().contains("--geo-fields"), "{}", err.message());

    handle.abort();
    bare.abort();
    mock.abort();
}
