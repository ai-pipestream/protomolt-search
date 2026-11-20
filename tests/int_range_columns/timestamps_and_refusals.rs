//! Timestamp epoch-micros conversion and loud integer ingest refusals.

use super::*;

/// Timestamp ingest is sugar over kind 4: the instant lands in the
/// named i64 column as epoch micros, queryable through range facets
/// with hand-computed micro edges, and every conversion that could lie
/// refuses instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamps_land_as_epoch_micros_in_the_integer_column() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        integer_fields: vec!["filed_at".to_string()],
        ..Default::default()
    })
    .await;

    // 2024-01-01T00:00:00Z and 2024-07-01T00:00:00Z, plus one instant
    // before the epoch. Hand-computed epoch micros: seconds * 1e6 plus
    // the nanos floor-divided by 1000.
    const JAN_2024: i64 = 1_704_067_200;
    const JUL_2024: i64 = 1_719_792_000;
    const PRE_EPOCH: i64 = -1_000;
    let docs: [(&str, i64, i32); 3] = [
        ("rust search rust fast", JAN_2024, 0),
        ("vector search rust", JUL_2024, 500_999),
        ("rust rust", PRE_EPOCH, 0),
    ];
    let send = |addr: String, reqs: Vec<AddDocumentsRequest>| async move {
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for r in reqs {
            tx.send(r).await.unwrap();
        }
        drop(tx);
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .map(|_| ())
    };
    let stamped = |text: &str, seconds: i64, nanos: i32| AddDocumentsRequest {
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
        timestamps: vec![TimestampValue {
            field: "filed_at".to_string(),
            value: Some(prost_types::Timestamp { seconds, nanos }),
        }],
        geo_points: Vec::new(),
        quality: None,
        geography: None,
        phrases: Vec::new(),
        phrase_fingerprint: 0,
        phrase_field: String::new(),
        position_fields: Vec::new(),
        bigram_fields: Vec::new(),
    };
    send(
        addr.clone(),
        docs.iter().map(|(t, s, n)| stamped(t, *s, *n)).collect(),
    )
    .await
    .unwrap();

    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(Some(analysis.clone()), Default::default());
    // Edges at 2024-01-01 and 2024-07-01, in micros. The pre-epoch
    // document falls below the first edge and lands nowhere; the July
    // document sits exactly on the last edge and also lands nowhere;
    // only January falls in the single bucket.
    let edges = vec![(JAN_2024 * 1_000_000) as f64, (JUL_2024 * 1_000_000) as f64];
    let (hits, _, ranges) = common::fanout::bm25_range_facets(
        &coordinator,
        "rust",
        10,
        &[range_field("filed_at", &edges)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(edges[0], edges[1], 1)],
        "only the January document falls inside [Jan, Jul)"
    );

    // Widening the upper edge by one microsecond brings July in: the
    // 500_999 ns remainder truncated to 500 micros, so the stored value
    // is JUL * 1e6 + 500, above the old edge and below the new one.
    let wider = vec![edges[0], edges[1] + 1_000.0];
    let (_, _, wide_ranges) = common::fanout::bm25_range_facets(
        &coordinator,
        "rust",
        10,
        &[range_field("filed_at", &wider)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(buckets_of(&wide_ranges[0]), vec![(wider[0], wider[1], 2)]);

    // Refusals: an unknown field, an overflowing instant, and a field
    // valued by BOTH lists in one document (they name the same column).
    let mut unknown = stamped("some text", JAN_2024, 0);
    unknown.timestamps[0].field = "filed".to_string();
    let err = send(addr.clone(), vec![unknown]).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("unknown integer field")
            && err.message().contains("--integer-fields"),
        "{}",
        err.message()
    );

    let overflow = stamped("some text", i64::MAX, 0);
    let err = send(addr.clone(), vec![overflow]).await.unwrap_err();
    assert!(
        err.message().contains("seconds") && err.message().contains("9999"),
        "{}",
        err.message()
    );

    for (seconds, nanos) in [
        (-62_135_596_801, 0),
        (253_402_300_800, 0),
        (i64::MIN, 0),
        (0, 1_000_000_000),
    ] {
        let error = send(addr.clone(), vec![stamped("some text", seconds, nanos)])
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("filed_at"), "{error}");
    }

    let bad_nanos = stamped("some text", JAN_2024, -1);
    let err = send(addr.clone(), vec![bad_nanos]).await.unwrap_err();
    assert!(err.message().contains("nanos"), "{}", err.message());

    let mut both = stamped("some text", JAN_2024, 0);
    both.integers.push(IntegerValue {
        field: "filed_at".to_string(),
        value: 1,
    });
    let err = send(addr.clone(), vec![both]).await.unwrap_err();
    assert!(
        err.message().contains("repeats") && err.message().contains("timestamps"),
        "integers and timestamps share the column, so a repeat spans both: {}",
        err.message()
    );

    handle.abort();
    mock.abort();
}

/// Unknown fields and repeated values are refused before anything mutates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integer_ingest_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;

    let cases: &[(Integers, &str)] = &[
        (&[("cite", 1)], "unknown integer field"),
        (&[("citations", 1), ("citations", 2)], "repeats"),
    ];
    for (integers, needle) in cases {
        let err = add_documents_integer(&addrs[0], &[("some text", integers)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // The unknown-field refusal names the knob, like every other column
    // typo refusal.
    let err = add_documents_integer(&addrs[0], &[("some text", &[("cite", 1)])])
        .await
        .unwrap_err();
    assert!(
        err.message().contains("--integer-fields"),
        "{}",
        err.message()
    );

    // A refused document left nothing behind: the shard still answers
    // the same range counts it did before.
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let (_, _, ranges) = common::fanout::bm25_range_facets(
        &coordinator,
        "rust",
        10,
        &[range_field("citations", &EDGES)],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(0.0, 10.0, 2), (10.0, 20.0, 1), (20.0, 30.0, 1)]
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}
