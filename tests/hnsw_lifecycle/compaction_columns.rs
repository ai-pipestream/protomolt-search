//! Multi-segment HNSW compaction with concurrent readers and fresh facet/numeric rows.

use super::*;

/// Multi-segment HNSW compaction merges linked segments via donor transplant +
/// Join-set merge under concurrent readers, and rebuilds lazy
/// `FacetRows` and `NumberRows` structures fresh against post-remap local doc
/// IDs so filtered queries never return stale pre-remap rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_merges_segments_and_rebuilds_facet_and_number_rows_fresh() {
    use pipestream_search::hnsw::HnswIndex;
    use pipestream_search::pb::{
        filter_query, CompositeSearchStrategy, FacetValue, FilterQuery, IntegerValue, NumericValue,
        SelectionOperator,
    };
    use std::sync::atomic::{AtomicBool, Ordering as Atomic};
    use std::sync::Arc;

    #[derive(Clone)]
    struct TypedRow {
        text: String,
        court: &'static str,
        year: i64,
        pages: f64,
        vector: Vec<f32>,
    }

    let dir = tempdir("merge-columns");
    let path = dir.join("shard.hnsw");
    let node_cfg = || NodeConfig {
        wal_buckets: 1,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["pages".into()],
        ..config(&path, true, true)
    };
    let (addr, handle) = start_empty_node(node_cfg()).await;
    configure(&addr).await;

    let courts = ["scotus", "ca9", "ca2"];
    let mut live: BTreeMap<u64, TypedRow> = BTreeMap::new();
    // 4 sealed segments of 320 rows each (>= MIN_GRAPH_ROWS) plus 60 unsealed tail rows.
    for (seg_idx, range) in [0..320usize, 320..640, 640..960, 960..1280, 1280..1340]
        .into_iter()
        .enumerate()
    {
        let mut docs = Vec::with_capacity(range.len());
        let mut vecs = Vec::with_capacity(range.len() * DIM);
        let mut batch_rows = Vec::with_capacity(range.len());
        for i in range.clone() {
            let court = courts[i % courts.len()];
            let year = 2000 + (i % 25) as i64;
            let pages = ((i * 7) % 100) as f64;
            let v = unit_vectors(1, DIM, SEED + 0x99_0000 + i as u64 * 131);
            let text = format!("opinion-{i} {court} y{year}");
            docs.push(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court.into(),
                }],
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: year,
                }],
                numerics: vec![NumericValue {
                    field: "pages".into(),
                    value: pages,
                }],
                ..Default::default()
            });
            vecs.extend_from_slice(&v);
            batch_rows.push(TypedRow {
                text,
                court,
                year,
                pages,
                vector: v,
            });
        }
        let mut cli = client(&addr).await;
        let d_res = cli
            .add_documents(tokio_stream::iter(docs))
            .await
            .unwrap()
            .into_inner();
        let v_res = cli
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: vecs,
                dim: DIM as u32,
            }]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(d_res.first_id, v_res.first_id);
        for (off, r) in batch_rows.into_iter().enumerate() {
            live.insert(d_res.first_id + off as u64, r);
        }
        if seg_idx < 4 {
            flush(&addr).await;
        }
    }

    // Delete ~25% of rows across all segments + tail so every post-compaction ID shifts.
    let doomed: Vec<u64> = (0..1340u64).filter(|id| id % 4 == 1).collect();
    let del_resp = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(del_resp.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }

    let c = coordinator(&addr);
    let filtered_query = |q_vec: &[f32], cel_expr: &str| QueryRequest {
        k: K as u32,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    speed_dense_leaf(q_vec),
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "flt".into(),
                            predicate: Some(filter_query::Predicate::Cel(cel_expr.into())),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    };

    type RowPred = fn(&TypedRow) -> bool;
    let predicates: &[(&str, RowPred)] = &[
        ("court == 'ca9'", |r| r.court == "ca9"),
        (
            "year >= 2005 && year <= 2018 && pages >= 20.0 && pages <= 80.0",
            |r| (2005..=2018).contains(&r.year) && (20.0..=80.0).contains(&r.pages),
        ),
        ("court == 'scotus' && pages >= 30.0", |r| {
            r.court == "scotus" && r.pages >= 30.0
        }),
    ];

    let probes = queries(6, 0x99C0_AA00);
    // Warm lazy FacetRows and NumberRows caches before compaction and verify pre-compaction recall.
    let verify_filtered = |stage: &'static str| {
        let addr_cl = addr.clone();
        let live_ref = &live;
        let probes_ref = &probes;
        async move {
            let coord = coordinator(&addr_cl);
            let mut cli = client(&addr_cl).await;
            for (p_idx, &(cel_expr, pred)) in predicates.iter().enumerate() {
                for (q_idx, q_vec) in probes_ref.iter().enumerate() {
                    let resp = coord
                        .query(Request::new(filtered_query(q_vec, cel_expr)))
                        .await
                        .unwrap()
                        .into_inner();
                    let docs = cli
                        .get_documents(GetDocumentsRequest {
                            doc_ids: resp.hits.iter().map(|h| h.doc_id).collect(),
                        })
                        .await
                        .unwrap()
                        .into_inner()
                        .documents;
                    let text_by_id: BTreeMap<u64, String> =
                        docs.into_iter().map(|d| (d.doc_id, d.text)).collect();
                    let by_text: BTreeMap<&str, &TypedRow> =
                        live_ref.values().map(|r| (r.text.as_str(), r)).collect();

                    let mut exact: Vec<(&str, f32)> = live_ref
                        .values()
                        .filter(|r| pred(r))
                        .map(|r| (r.text.as_str(), dot(&r.vector, q_vec)))
                        .collect();
                    exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(b.0)));
                    exact.truncate(K);

                    assert_eq!(
                        resp.hits.len(),
                        exact.len(),
                        "{stage} pred={p_idx} q={q_idx}"
                    );
                    let mut got_texts = Vec::with_capacity(resp.hits.len());
                    for hit in &resp.hits {
                        let txt = text_by_id
                            .get(&hit.doc_id)
                            .unwrap_or_else(|| panic!("{stage}: missing doc {}", hit.doc_id));
                        let row = by_text
                            .get(txt.as_str())
                            .unwrap_or_else(|| panic!("{stage}: returned deleted row {txt}"));
                        assert!(pred(row), "{stage}: row {txt} failed predicate {cel_expr}");
                        assert_eq!(
                            hit.score.to_bits(),
                            dot(&row.vector, q_vec).to_bits(),
                            "{stage}: exact score mismatch on {txt}"
                        );
                        got_texts.push(txt.as_str());
                    }
                    let recall_hits = exact.iter().filter(|(t, _)| got_texts.contains(t)).count();
                    assert!(
                        recall_hits >= exact.len().saturating_sub(2),
                        "{stage} pred={p_idx} q={q_idx}: recall {recall_hits}/{}",
                        exact.len()
                    );
                }
            }
        }
    };

    verify_filtered("pre-compact").await;

    // Concurrent background reader during CompactShard cutover.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_reader = Arc::clone(&stop);
    let reader_addr = addr.clone();
    let reader_probe = probes[0].clone();
    let reader_task = tokio::spawn(async move {
        let coord = coordinator(&reader_addr);
        let mut iterations = 0usize;
        while !stop_reader.load(Atomic::Relaxed) {
            let q_res = coord
                .query(Request::new(QueryRequest {
                    k: K as u32,
                    selection: Some(speed_dense_leaf(&reader_probe)),
                    ..Default::default()
                }))
                .await;
            match q_res {
                Ok(resp) => assert_eq!(resp.into_inner().hits.len(), K),
                // Two-phase Query rightly rejects cross-cutover slot reuse when
                // `stats_epoch` advances between scan and document fetch.
                Err(st)
                    if st.code() == Code::FailedPrecondition
                        && st.message().contains("query data changed during execution") =>
                {
                    continue;
                }
                Err(st) => panic!("unexpected concurrent Query error during compaction: {st:?}"),
            }
            let s_hits = streamed(&coord, "concurrent-compact", &reader_probe, K).await;
            assert_eq!(s_hits.len(), K);
            iterations += 1;
            tokio::task::yield_now().await;
        }
        iterations
    });

    let mut req = Request::new(CompactShardRequest::default());
    req.set_timeout(Duration::from_secs(600));
    let compacted = client(&addr)
        .await
        .compact_shard(req)
        .await
        .unwrap()
        .into_inner();
    stop.store(true, Atomic::Relaxed);
    let concurrent_reads = reader_task.await.unwrap();
    assert!(concurrent_reads > 0);
    assert_eq!(compacted.rows_before, 1340);
    assert_eq!(compacted.rows_after as usize, live.len());
    assert_eq!(compacted.tombstones_reclaimed as usize, doomed.len());

    // Post-compaction filtered queries must build FacetRows and NumberRows fresh
    // on the merged segment and return the exact live filtered rows.
    let _ = &c;
    verify_filtered("post-compact").await;

    handle.abort();
    let root = segments_root(&path);
    let reopened = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert_eq!(
        reopened.len(),
        1,
        "4 segments + tail compacted into 1 segment"
    );
    let seg_meta = reopened.metadata(0);
    let seg_vec_path =
        SegmentCatalog::segment_dir(&root, &seg_meta.segment_id).join(&seg_meta.vector.file);
    let mapped_hnsw = HnswIndex::load_mapped(&seg_vec_path).unwrap();
    assert!(mapped_hnsw.is_graph_linked());
    assert_eq!(mapped_hnsw.unreachable_from_entry(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}
