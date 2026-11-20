//! Single-column, multi-key, and un-scored lexical membership sorting with pagination.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_browse_orders_by_column_and_pages() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let selection = || cel_filter("f", "year >= 1");
    let by = |column: &str, descending: bool| QuerySort {
        map: None,
        column: column.into(),
        descending,
    };

    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(selection()),
            sort: vec![by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let rows: Vec<(u64, f64)> = full.hits.iter().map(|h| (h.doc_id, h.sort_key)).collect();
    assert_eq!(
        rows,
        (1..8)
            .rev()
            .map(|i| (i as u64, i as f64))
            .collect::<Vec<_>>(),
        "newest first, and the sort key is reported"
    );
    assert_eq!(
        full.hits[0].sort_values,
        vec![pipestream_search::pb::SortValue {
            value: Some(pipestream_search::pb::sort_value::Value::Integer(7))
        }],
        "the typed value rides alongside"
    );

    // Ascending, paged: stitches into the ascending order.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(selection()),
                sort: vec![by("year", false)],
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
        assert!(resp.next_cursor.starts_with("pqc1:"), "sealed sorted token");
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        (1..8).map(|i| (i as u64, i as u32)).collect::<Vec<_>>(),
        "sorted pages stitch with continuing ranks"
    );

    // Refusals: unknown sort column by name; sort on a dense shape.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(selection()),
            sort: vec![by("yaer", false)],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("yaer"), "{}", err.message());
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(dense_leaf("d", &_qvec)),
            sort: vec![by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("no membership to order"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_sort_orders_text_then_number_and_pages() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let by = |column: &str, descending: bool| QuerySort {
        map: None,
        column: column.into(),
        descending,
    };
    // court ascending (ca2 < ca9 < scotus), then year descending.
    let expected: Vec<(u64, &str)> = vec![
        (7, "ca2"),
        (4, "ca2"),
        (1, "ca2"),
        (6, "ca9"),
        (3, "ca9"),
        (0, "ca9"),
        (5, "scotus"),
        (2, "scotus"),
    ];
    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("court", false), by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let got: Vec<(u64, String)> = full
        .hits
        .iter()
        .map(|h| {
            let text = match h.sort_values[0].value.as_ref().unwrap() {
                pipestream_search::pb::sort_value::Value::Text(t) => t.clone(),
                other => panic!("{other:?}"),
            };
            (h.doc_id, text)
        })
        .collect();
    assert_eq!(
        got,
        expected
            .iter()
            .map(|(id, c)| (*id, c.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        full.hits[0].sort_key, 0.0,
        "a text first key has no numeric view"
    );

    // Text descending reverses the court order and keeps the year order.
    let reversed = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("court", true), by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = reversed.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![5, 2, 6, 3, 0, 7, 4, 1]);

    // Paged by 3 with a text boundary in the cursor: stitches exactly.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(cel_filter("f", "year >= 0")),
                sort: vec![by("court", false), by("year", true)],
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| h.doc_id));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        expected.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );

    // A lineage key sorts too: parent_id descending, then id.
    let parents = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("parent_id", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = parents.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![6, 7, 4, 5, 2, 3, 0, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lexical_leaf_sorts_its_exact_membership_without_scores() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    // "zebra" matches the even ids; year descending orders them 6,4,2,0.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(lexical_leaf("lex", "zebra")),
            sort: vec![QuerySort {
                map: None,
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![6, 4, 2, 0]);
    assert!(
        resp.hits.iter().all(|h| h.score == 0.0),
        "no relevance is computed"
    );
    assert_eq!(resp.hits[0].matched, vec!["lex".to_string()]);
    assert_eq!(resp.executed, "browse_shard:lexical");

    // With a filter: the membership is the AND of the leaf and the filter.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(composite(
                SelectionOperator::And,
                vec![lexical_leaf("lex", "zebra"), cel_filter("f", "year >= 3")],
                None,
            )),
            sort: vec![QuerySort {
                map: None,
                column: "year".into(),
                descending: false,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![4, 6]);
    assert_eq!(
        resp.hits[0].matched,
        vec!["lex".to_string(), "f".to_string()]
    );

    // Paged by 1, stitching through the sorted lexical membership.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 1,
                selection: Some(lexical_leaf("lex", "zebra")),
                sort: vec![QuerySort {
                    map: None,
                    column: "year".into(),
                    descending: true,
                }],
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
    assert_eq!(stitched, vec![(6, 1), (4, 2), (2, 3), (0, 4)]);

    // A relevance shape on the sorted leaf refuses by name.
    let mut phrase = lexical_leaf("lex", "zebra document");
    if let Some(selection_query::Node::Search(SearchQuery {
        query: Some(search_query::Query::Lexical(q)),
        ..
    })) = phrase.node.as_mut()
    {
        q.phrase = Some(pipestream_search::pb::PhraseMatch::default());
    }
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(phrase),
            sort: vec![QuerySort {
                map: None,
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("a phrase"), "{}", err.message());
}
