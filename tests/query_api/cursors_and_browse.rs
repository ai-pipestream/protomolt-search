//! Stateless cursor pagination, corpus-change refusal, and filter-only ID-order browse.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_stitch_into_the_full_ranking() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let leaf = || lexical_leaf("lex", "document");

    let full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(leaf()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.hits.len(), 8, "every doc says \"document\"");

    let mut stitched = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(leaf()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(
            resp.hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits(), h.rank)),
        );
        pages += 1;
        if resp.next_cursor.is_empty() {
            assert!(resp.hits.len() < 3, "a full page always mints a cursor");
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(pages, 3, "8 hits in pages of 3");
    let want: Vec<(u64, u32, u32)> = full
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank))
        .collect();
    assert_eq!(stitched, want, "pages stitch bitwise, ranks absolute");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_pages_within_its_fixed_pool() {
    let (coordinator, qvec, _handles) = start_fleet().await;
    let selection = || {
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        )
    };

    let full = query(
        &coordinator,
        QueryRequest {
            k: 6,
            selection_k: 6,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut stitched = Vec::new();
    let mut cursor = String::new();
    for _ in 0..3 {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 2,
                selection_k: 6,
                selection: Some(selection()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.hits.len(), 2);
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.score.to_bits())));
        cursor = resp.next_cursor.clone();
        assert!(!cursor.is_empty());
    }
    let want: Vec<(u64, u32)> = full
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect();
    assert_eq!(stitched, want, "pages of the fixed pool stitch bitwise");

    // The pool is exhausted; deepening would change the ranking, so
    // the refusal names the knob.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 6,
            selection: Some(selection()),
            cursor,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("selection_k"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_corpus_refuses_the_cursor() {
    let ((coordinator, _qvec, _handles), addrs) = start_fleet_with_addrs().await;
    let leaf = || lexical_leaf("lex", "document");

    let first = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!first.next_cursor.is_empty());

    // Ingest changes the bound physical read version. Refuse the cursor
    // before re-executing selection, even if its boundary could still match.
    let mut client = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(AddDocumentsRequest {
        text: "a brand new document about zebras".into(),
        integers: vec![IntegerValue {
            field: "year".into(),
            value: 99,
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            cursor: first.next_cursor,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("cursor data context changed"),
        "{}",
        err.message()
    );

    // A malformed token refuses as malformed, not as a corpus change.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            cursor: "not-a-cursor".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("malformed cursor"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_only_browse_pages_in_id_order() {
    let (coordinator, _qvec, _handles) = start_fleet().await;

    // One bare filter is a browse; docs 2..=6 pass, ids ascending.
    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 2 && year <= 6")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.executed, "browse");
    let ids: Vec<u64> = full.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![2, 3, 4, 5, 6], "deterministic id order");
    assert!(full.next_cursor.is_empty(), "short page: nothing follows");
    for hit in &full.hits {
        assert_eq!(hit.score, 0.0, "no relevance score exists on this route");
        assert!(hit.signals.is_empty(), "a filter is never a signal");
        assert_eq!(hit.matched, vec!["f"]);
    }

    // Filters under an AND wrapper conjoin, and pages stitch with
    // continuing ranks.
    let selection = || {
        composite(
            SelectionOperator::And,
            vec![
                cel_filter("low", "year >= 1"),
                cel_filter("high", "year <= 6"),
            ],
            None,
        )
    };
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 4,
                selection: Some(selection()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)],
        "pages stitch; ranks continue across pages"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browse_refuses_an_unknown_column_by_name() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(cel_filter("f", "yaer >= 2")),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("yaer"), "{}", err.message());
}
