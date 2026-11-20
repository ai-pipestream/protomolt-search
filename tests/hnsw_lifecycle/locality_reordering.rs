//! End-to-end HNSW locality row reordering across seal, online compaction,
//! column filters, collapse modes, and restart.

use super::locality_support::{eval_locality_queries, PreCompactSnapshot, RichRow};
use super::*;
use pipestream_search::hnsw::{BuildMode, HnswIndex, LocalityOrder};
use pipestream_search::pb::{DocLineage, FacetValue, IntegerValue, NumericValue};

const COURTS: [&str; 4] = ["scotus", "ca9", "ca2", "ca5"];

fn rich_doc_request(r: &RichRow) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: r.text.clone(),
        analysis: Some(body_spec()),
        facets: vec![FacetValue {
            field: "court".into(),
            value: r.court.into(),
        }],
        integers: vec![IntegerValue {
            field: "year".into(),
            value: r.year,
        }],
        numerics: vec![NumericValue {
            field: "pages".into(),
            value: r.pages,
        }],
        lineage: Some(DocLineage {
            parent_id: r.parent_id,
            group_id: r.parent_id,
            span_start: 0,
            span_end: 10,
        }),
        ..Default::default()
    }
}

fn build_rich_row(i: usize) -> RichRow {
    let court = COURTS[i % COURTS.len()];
    let year = 2000 + (i % 25) as i64;
    let pages = ((i * 11) % 100) as f64;
    let parent_id = 1_000 + (i / 3) as u64;
    let topic = i % 8;
    let mut v = unit_vectors(1, DIM, SEED + 0x98_0000 + i as u64 * 137);
    v[topic * 3] += 2.5;
    let v_norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in &mut v {
        *x /= v_norm;
    }
    RichRow {
        text: format!("doc-{i} {court} y{year} p{parent_id}"),
        court,
        year,
        pages,
        parent_id,
        vector: v,
    }
}

async fn seed_locality_segments(addr: &str) -> BTreeMap<u64, RichRow> {
    let mut live = BTreeMap::new();
    for (seg_idx, range) in [0..320usize, 320..640, 640..680].into_iter().enumerate() {
        let rows_batch: Vec<RichRow> = range.map(build_rich_row).collect();
        let docs: Vec<AddDocumentsRequest> = rows_batch.iter().map(rich_doc_request).collect();
        let vecs: Vec<f32> = rows_batch
            .iter()
            .flat_map(|r| r.vector.iter().copied())
            .collect();
        let mut cli = client(addr).await;
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
        for (off, r) in rows_batch.into_iter().enumerate() {
            live.insert(d_res.first_id + off as u64, r);
        }
        if seg_idx < 2 {
            flush(addr).await;
        }
    }
    live
}

async fn mutate_locality_rows(addr: &str, live: &mut BTreeMap<u64, RichRow>) {
    let doomed: Vec<u64> = (0..680u64).filter(|id| id % 6 == 2).collect();
    let del_res = client(addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(del_res.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }

    for rep_old in [5u64, 121, 333, 511, 645] {
        let old_row = live.remove(&rep_old).unwrap();
        let replacement = RichRow {
            text: format!("{}-replaced", old_row.text),
            vector: unit_vectors(1, DIM, SEED + 0x98_BEEF + rep_old * 97),
            ..old_row
        };
        let mut cli = client(addr).await;
        let d_res = cli
            .add_documents(tokio_stream::iter(vec![rich_doc_request(&replacement)]))
            .await
            .unwrap()
            .into_inner();
        let v_res = cli
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: replacement.vector.clone(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(d_res.first_id, v_res.first_id);
        let committed = cli
            .commit_replacements(CommitReplacementsRequest {
                replacements: vec![Replacement {
                    old_doc_id: rep_old,
                    new_doc_id: d_res.first_id,
                }],
                expected_wal_generation: None,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(committed.committed, 1);
        live.insert(d_res.first_id, replacement);
    }
}

fn verify_sealed_hnsw(root: &Path, order: LocalityOrder) {
    let opened = OpenedSegmentSet::open_with(root, VectorLoad::Mapped).unwrap();
    assert_eq!(opened.len(), 2);
    for seg_i in 0..opened.len() {
        let meta = opened.metadata(seg_i);
        let vec_p = SegmentCatalog::segment_dir(root, &meta.segment_id).join(&meta.vector.file);
        let mapped = HnswIndex::load_mapped(&vec_p).unwrap();
        assert_eq!(mapped.config().locality_order, order);
        assert_eq!(
            mapped.has_node_map(),
            order != LocalityOrder::None,
            "sealed segment {seg_i} node-map flag for {order:?}"
        );
    }
}

fn verify_compacted_hnsw(root: &Path, order: LocalityOrder) -> f64 {
    let opened = OpenedSegmentSet::open_with(root, VectorLoad::Mapped).unwrap();
    assert_eq!(opened.len(), 1);
    let meta = opened.metadata(0);
    let vec_p = SegmentCatalog::segment_dir(root, &meta.segment_id).join(&meta.vector.file);
    let mapped = HnswIndex::load_mapped(&vec_p).unwrap();
    assert!(mapped.is_graph_linked());
    assert_eq!(mapped.config().locality_order, order);
    assert!(
        !mapped.has_node_map(),
        "compacted segment must use global slot order without NodeMap"
    );
    assert_eq!(mapped.unreachable_from_entry(), 0);
    mapped.locality_metrics().mean_id_gap
}

async fn run_locality_order(
    order: LocalityOrder,
    probes: &[Vec<f32>],
) -> (PreCompactSnapshot, PreCompactSnapshot, f64) {
    let tag = format!("reorder-{order:?}");
    let dir = tempdir(&tag);
    let path = dir.join("shard.hnsw");
    let node_cfg = || NodeConfig {
        wal_buckets: 1,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["pages".into()],
        ..config(&path, true, true)
    };
    let (addr, handle) = start_empty_node(node_cfg()).await;
    let hnsw_cfg = HnswConfig {
        m: 12,
        ef_construction: 96,
        seed: 42,
        build_mode: BuildMode::FixedOrder,
        locality_order: order,
    }
    .backend_config()
    .unwrap();
    client(&addr)
        .await
        .configure_vector_backend(ConfigureVectorBackendRequest {
            dim: DIM as u32,
            config: Some(VectorBackendConfig {
                backend_kind: hnsw_cfg.backend_kind,
                config_format: hnsw_cfg.config_format,
                payload: hnsw_cfg.payload,
            }),
        })
        .await
        .unwrap();

    let mut live = seed_locality_segments(&addr).await;
    mutate_locality_rows(&addr, &mut live).await;

    let root = segments_root(&path);
    verify_sealed_hnsw(&root, order);
    let pre_snap = eval_locality_queries(&addr, &live, probes, &format!("{order:?}-pre")).await;

    let mut req = Request::new(CompactShardRequest::default());
    req.set_timeout(Duration::from_secs(600));
    let compacted = client(&addr)
        .await
        .compact_shard(req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(compacted.rows_after as usize, live.len());

    let post_snap = eval_locality_queries(&addr, &live, probes, &format!("{order:?}-post")).await;
    handle.abort();

    let compacted_gap = verify_compacted_hnsw(&root, order);
    let (re_addr, re_handle) = start_opened_node(node_cfg()).await;
    let restart_snap =
        eval_locality_queries(&re_addr, &live, probes, &format!("{order:?}-restart")).await;
    assert_eq!(restart_snap, post_snap, "{order:?} restart mismatch");
    re_handle.abort();
    (pre_snap, post_snap, compacted_gap)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locality_reordering_round_trips_seal_compaction_collapse_and_column_filters() {
    let probes = queries(4, 0x98C0_FFEE);
    let (none_pre, none_post, none_gap) = run_locality_order(LocalityOrder::None, &probes).await;
    for order in [LocalityOrder::Rcm, LocalityOrder::RecursiveBisection] {
        let (pre, post, gap) = run_locality_order(order, &probes).await;
        assert_eq!(
            pre, none_pre,
            "{order:?} pre-compaction seal answers diverged from None"
        );
        assert!(
            gap < none_gap * 0.90,
            "{order:?} compacted mean_id_gap {gap:.1} did not beat None {none_gap:.1}"
        );
        for qi in 0..probes.len() {
            assert_eq!(post.unfiltered[qi].len(), none_post.unfiltered[qi].len());
            assert_eq!(post.facet[qi].len(), none_post.facet[qi].len());
            assert_eq!(
                post.number_range[qi].len(),
                none_post.number_range[qi].len()
            );
            assert_eq!(
                post.col_collapse[qi].len(),
                none_post.col_collapse[qi].len()
            );
            assert_eq!(
                post.parent_collapse[qi].len(),
                none_post.parent_collapse[qi].len()
            );
        }
    }
}
