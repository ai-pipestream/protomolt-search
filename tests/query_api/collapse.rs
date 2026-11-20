//! Field-collapsed top-k grouping with inner hits and cursor pagination.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collapse_groups_the_pool_by_key_with_inner_hits_and_pages() {
    let (coordinator, qvec, _handles) = start_fleet().await;
    let collapse = |column: &str, inner_hits: u32| {
        Some(CollapseSpec {
            map: None,
            column: column.into(),
            inner_hits,
        })
    };
    // The uncollapsed dense order over the whole corpus is the oracle:
    // groups appear in the order of their best hit.
    let full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(dense_leaf("d", &qvec)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.hits.len(), 8);
    let mut expected_groups: Vec<(u64, Vec<u64>)> = Vec::new();
    for h in &full.hits {
        let parent = h.doc_id / 2;
        match expected_groups.iter_mut().find(|(p, _)| *p == parent) {
            Some((_, members)) => members.push(h.doc_id),
            None => expected_groups.push((parent, vec![h.doc_id])),
        }
    }
    assert_eq!(expected_groups.len(), 4);

    // k = 2 groups by parent over a single dense leaf: the pool deepens
    // from k until two parents show, and the representatives are the
    // groups' best hits in relevance order.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.executed, "search+collapse");
    let reps: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(
        reps,
        expected_groups
            .iter()
            .take(2)
            .map(|(_, m)| m[0])
            .collect::<Vec<_>>()
    );
    assert_eq!(resp.groups.len(), 2);
    for (i, (group, (parent, _))) in resp.groups.iter().zip(&expected_groups).enumerate() {
        assert_eq!(
            group.key,
            Some(pipestream_search::pb::SortValue {
                value: Some(pipestream_search::pb::sort_value::Value::UnsignedInteger(
                    *parent
                ))
            })
        );
        assert!(group.hits.len() <= 2 && !group.hits.is_empty());
        assert_eq!(
            group.hits[0].doc_id, reps[i],
            "the representative leads its group"
        );
        assert_eq!(group.hits[0].rank, 1, "ranks count within the group");
        assert!(group.hits.iter().all(|h| h.doc_id / 2 == *parent));
    }

    // A pool deeper than the corpus comes back short, which is the proof
    // that every group is complete; a pool exactly the corpus's size
    // cannot tell the end from a cut and reports incomplete groups.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 8,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        resp.groups.iter().all(|g| !g.complete),
        "a full pool proves nothing about what follows"
    );
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 16,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.hits.len(), 4);
    for (group, (parent, members)) in resp.groups.iter().zip(&expected_groups) {
        let listed: Vec<u64> = group.hits.iter().map(|h| h.doc_id).collect();
        assert_eq!(&listed, members, "parent {parent}");
        assert_eq!(group.pool_hits, 2);
        assert!(group.complete, "the pool reached the end of the corpus");
    }

    // Paging by one group stitches into the full group order.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 1,
                selection: Some(dense_leaf("d", &qvec)),
                collapse: collapse("parent_id", 0),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        assert!(resp.groups.iter().all(|g| g.hits.len() == 1));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        expected_groups
            .iter()
            .enumerate()
            .map(|(i, (_, m))| (m[0], (i + 1) as u32))
            .collect::<Vec<_>>()
    );

    // A facet key on a lexical leaf: "document" matches everything, so
    // the groups are the three courts in the order of their best hit.
    let lexical_full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut courts_in_order: Vec<&str> = Vec::new();
    for h in &lexical_full.hits {
        let court = COURTS[h.doc_id as usize % 3];
        if !courts_in_order.contains(&court) {
            courts_in_order.push(court);
        }
    }
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(lexical_leaf("lex", "document")),
            collapse: collapse("court", 1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let keys: Vec<String> = resp
        .groups
        .iter()
        .map(|g| match g.key.as_ref().unwrap().value.as_ref().unwrap() {
            pipestream_search::pb::sort_value::Value::Text(t) => t.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(keys, courts_in_order);
    assert_eq!(resp.executed, "bm25_search+collapse");

    // A composite is a fixed pool: selection_k = 8 holds every parent;
    // a filtered pool of four hits that all share one lineage group
    // cannot show two groups and refuses naming selection_k.
    let composite_of = |selection_k: u32, k: u32, column: &str, filter: Option<SelectionQuery>| {
        let fused = composite(
            SelectionOperator::Or,
            vec![dense_leaf("d", &qvec), lexical_leaf("lex", "document")],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        );
        QueryRequest {
            k,
            selection_k,
            selection: Some(match filter {
                Some(f) => composite(SelectionOperator::And, vec![fused, f], None),
                None => fused,
            }),
            collapse: collapse(column, 2),
            ..Default::default()
        }
    };
    let resp = query(&coordinator, composite_of(8, 4, "parent_id", None))
        .await
        .unwrap();
    assert_eq!(resp.hits.len(), 4);
    assert_eq!(resp.groups.len(), 4);
    assert_eq!(resp.executed, "hybrid_search:global_rank+collapse");
    let err = query(
        &coordinator,
        composite_of(4, 2, "group_id", Some(cel_filter("f", "year < 4"))),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        err.message()
    );
    assert!(err.message().contains("selection_k"), "{}", err.message());

    // Refusals by name: a browse has no order to collapse by; collapse
    // and sort do not combine.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(cel_filter("f", "year >= 0")),
            collapse: collapse("court", 1),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("SCORED selection"),
        "{}",
        err.message()
    );
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(lexical_leaf("lex", "document")),
            collapse: collapse("court", 1),
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
    assert!(
        err.message().contains("do not combine"),
        "{}",
        err.message()
    );
}
