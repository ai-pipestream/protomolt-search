//! Multi-plane landing, bind/schema gap refusals, enum unknowns, and lockstep checks.

use super::*;

// The tests

/// Every plane at once: text body, multi-field text, string / enum /
/// bool facets, i64 (plain, timestamp-sourced, and nested), f64, and
/// the vector leg at the SAME ids — through one mapped stream.
#[tokio::test]
async fn mapped_ingest_lands_on_every_plane() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let response = send(
        &addr,
        bind(),
        (0..N_DOCS).map(|i| doc(i).encode()).collect(),
    )
    .await
    .expect("mapped ingest succeeds");
    assert_eq!(response.added, N_DOCS as u64);
    assert_eq!(response.total, N_DOCS as u64);
    assert_eq!(response.first_id, 0);
    assert_eq!(
        response.fingerprint,
        derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint
    );

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());

    // Projections read back every column, absences included.
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            projections: [
                "price",
                "year",
                "created_at",
                "meta_page_count",
                "id",
                "status",
                "published",
            ]
            .iter()
            .map(|name| NamedProjection {
                name: (*name).to_string(),
                expression: (*name).to_string(),
            })
            .collect(),
            ..Default::default()
        }))
        .await
        .expect("projections over mapped columns")
        .into_inner()
        .hits;
    assert_eq!(hits.len(), N_DOCS);
    for hit in &hits {
        let i = hit.doc_id as usize;
        let d = doc(i);
        let values: Vec<Option<projected_value::Value>> =
            hit.projected.iter().map(|p| p.value.clone()).collect();
        use projected_value::Value::{DoubleValue, IntValue, StringValue};
        assert_eq!(values[0], d.price.map(DoubleValue), "price of doc {i}");
        assert_eq!(values[1], d.year.map(IntValue), "year of doc {i}");
        let (seconds, nanos) = d.created_at.unwrap();
        assert_eq!(
            values[2],
            Some(IntValue(seconds * 1_000_000 + i64::from(nanos) / 1_000)),
            "created_at of doc {i}"
        );
        assert_eq!(
            values[3],
            Some(IntValue(10 * i as i64 + 5)),
            "meta_page_count of doc {i}"
        );
        assert_eq!(
            values[4],
            Some(StringValue(format!("case-{i}"))),
            "id of doc {i}"
        );
        assert_eq!(
            values[5],
            d.status
                .map(|s| StringValue(if s == 1 { "OPEN" } else { "CLOSED" }.to_string())),
            "status of doc {i}"
        );
        assert_eq!(
            values[6],
            d.published.then(|| StringValue("true".to_string())),
            "published of doc {i}"
        );
    }

    // CEL selects over the mapped columns with the documented absence
    // rule: doc 2 (no year) and doc 3 (no price) never match.
    let filtered: Vec<u64> = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            filter: "year >= 1992 && price > 0.0".into(),
            ..Default::default()
        }))
        .await
        .expect("CEL filter over mapped columns")
        .into_inner()
        .hits
        .iter()
        .map(|h| h.doc_id)
        .collect();
    let mut filtered = filtered;
    filtered.sort_unstable();
    assert_eq!(filtered, vec![4, 5], "1994 and 1995, priced");

    // The vector leg landed at the SAME ids: querying doc 2's own
    // embedding returns doc 2 first.
    let top = coordinator
        .search(Request::new(SearchRequest {
            k: 1,
            vector: embedding_of(2),
            ..Default::default()
        }))
        .await
        .expect("vector search over mapped ingest")
        .into_inner()
        .hits;
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].vector_id, 2, "the vector leg shares the doc's id");
}

/// The bind stands or nothing streams: fingerprint discipline, declared
/// columns, body selection, protocol order.
#[tokio::test]
async fn bind_refusals_name_the_gap() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    // Fingerprint required: dry-run first, then bind what you saw.
    let mut unbound = bind();
    unbound.expected_fingerprint = String::new();
    expect_refusal(
        send(&addr, unbound, vec![]).await,
        "expected_fingerprint is required",
    );

    // A stale fingerprint names both sides.
    let real = derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint;
    let mut stale = bind();
    stale.expected_fingerprint = "deadbeef".into();
    expect_refusal(send(&addr, stale, vec![]).await, &real);

    // Two TEXT fields and no body_path: refused naming both.
    let mut ambiguous = bind();
    ambiguous.body_path = String::new();
    expect_refusal(send(&addr, ambiguous, vec![]).await, "meta.author");

    // body_path must be one of the plan's TEXT fields.
    let mut wrong_body = bind();
    wrong_body.body_path = "price".into();
    expect_refusal(send(&addr, wrong_body, vec![]).await, "TEXT fields");

    // A shard that does not declare the landing columns refuses at
    // bind, listing every gap with its flag.
    let (bare_addr, _bare) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into(), "year".into()],
        ..Default::default()
    })
    .await;
    expect_refusal(
        send(&bare_addr, bind(), vec![]).await,
        "\"price\" (--numeric-fields)",
    );

    // A plan that lands a source field in a DERIVED column
    // is refused at bind, by name: the index
    // computes that column itself.
    let derived =
        pipestream_search::derived::Declaration::compile(&pipestream_search::pb::DerivedColumns {
            columns: vec![pipestream_search::pb::DerivedColumn {
                name: "year".into(),
                expression: "meta_page_count + 1900".into(),
                kind: pipestream_search::pb::MaterializeKind::I64 as i32,
                disclosure: pipestream_search::pb::DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap();
    let (derived_addr, _derived) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into()],
        numeric_fields: vec!["price".into()],
        derived: Some(std::sync::Arc::new(derived)),
        ..Default::default()
    })
    .await;
    expect_refusal(
        send(&derived_addr, bind(), vec![]).await,
        "lands as \"year\", a derived column",
    );

    // Protocol: the bind comes first, exactly once.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(IngestMappedRequest {
        payload: Some(ingest_mapped_request::Payload::Document(doc(0).encode())),
    })
    .await
    .unwrap();
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a document before the bind is refused");
    assert!(status.message().contains("must be a MappedBind"));

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    for payload in [
        ingest_mapped_request::Payload::Bind(bind()),
        ingest_mapped_request::Payload::Bind(bind()),
    ] {
        tx.send(IngestMappedRequest {
            payload: Some(payload),
        })
        .await
        .unwrap();
    }
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a second bind is refused");
    assert!(status.message().contains("bind repeats"));
}

/// Documents that cannot land refuse by position and by field; the
/// shard stays usable afterwards.
#[tokio::test]
async fn document_refusals_name_position_and_field() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    let mut no_vector = doc(0);
    no_vector.embedding = Vec::new();
    expect_refusal(
        send(&addr, bind(), vec![no_vector.encode()]).await,
        "document 0",
    );
    expect_refusal(
        send(&addr, bind(), vec![no_vector.encode()]).await,
        "has no vector",
    );

    let mut no_id = doc(0);
    no_id.id = None;
    expect_refusal(send(&addr, bind(), vec![no_id.encode()]).await, "has no id");

    let mut no_body = doc(0);
    no_body.title = None;
    expect_refusal(
        send(&addr, bind(), vec![no_body.encode()]).await,
        "no body text",
    );

    let mut truncated = doc(0).encode();
    truncated.truncate(truncated.len() - 3);
    expect_refusal(send(&addr, bind(), vec![truncated]).await, "document 0");

    let mut short = doc(1);
    short.embedding = embedding_of(1)[..4].to_vec();
    expect_refusal(
        send(&addr, bind(), vec![doc(0).encode(), short.encode()]).await,
        "4 floats",
    );

    // Nothing above poisoned the shard: a clean stream still lands.
    // (The dim-mismatch stream above applied its first document before
    // refusing the second, exactly like ordinary ingest.)
    let response = send(&addr, bind(), vec![doc(2).encode()])
        .await
        .expect("the shard ingests after refusals");
    assert_eq!(response.added, 1);
}

#[tokio::test]
async fn closed_enum_unknowns_preserve_presence_and_prior_values_in_query() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let mut unknown = doc(0);
    unknown.status = Some(99);
    let mut known_then_unknown = doc(1).encode();
    known_then_unknown.extend([40, 99]);
    let mut unknown_then_known = unknown.encode();
    unknown_then_known.extend([40, 1]);
    let response = send(
        &addr,
        bind(),
        vec![unknown.encode(), known_then_unknown, unknown_then_known],
    )
    .await
    .unwrap();
    assert_eq!(response.added, 3);
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: 3,
            projections: vec![NamedProjection {
                name: "status".into(),
                expression: "status".into(),
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 3);
    for hit in hits {
        let expected = match hit.doc_id {
            0 => None,
            1 => Some(projected_value::Value::StringValue("CLOSED".into())),
            2 => Some(projected_value::Value::StringValue("OPEN".into())),
            other => panic!("unexpected row {other}"),
        };
        assert_eq!(hit.projected[0].value, expected);
    }
}

/// A shard whose document leg ran ahead cannot take mapped documents:
/// the vector would land below its document. Refused by name.
#[tokio::test]
async fn lockstep_refusal_when_document_leg_is_ahead() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    // Bind while empty; this test targets alignment after an established bind.
    send(&addr, bind(), vec![]).await.unwrap();

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddDocumentsRequest {
        text: "a plain document with no vector".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    expect_refusal(send(&addr, bind(), vec![doc(0).encode()]).await, "lockstep");
}
