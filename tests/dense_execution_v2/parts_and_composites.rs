//! Segmented HNSW part accounting and real composite dense-leg receipts.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_only_and_mixed_segmented_hnsw_report_honest_part_counts() {
    let dir = DirGuard::new("hnsw_parts");
    let index = HnswIndex::new(DIM, HnswConfig::default())
        .unwrap()
        .into_index();
    let (addr, handle) = start_node(index, hnsw_segmented_config(dir.0.join("s0.idx"), 0)).await;
    append_rows(&addr, &unit_vectors(24, DIM, 0x7A11_0001), 0, "ca9").await;
    let qvec = unit_vectors(1, DIM, 0x7A11_0002);
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(1);

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let req = QueryRequest {
            k: 5,
            selection: Some(dense_leaf(&qvec, Mode::Balanced, Precision::Auto)),
            ..Default::default()
        };
        let out = run_query(&coord, req).await.dense_execution.unwrap();
        assert_eq!(out.coverage, DenseCoverage::CandidateSet as i32);
        assert_eq!(out.termination, DenseTermination::Exhausted as i32);
        assert_eq!(out.tail_enumerated_rows, 24);
        assert_eq!(
            out.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 1)]
        );
    }

    assert_mixed_and_empty_hnsw_parts(&addr, &coordinator, &qvec).await;
    handle.abort();
}

async fn assert_mixed_and_empty_hnsw_parts(
    addr: &str,
    coordinator: &CoordinatorServiceImpl,
    qvec: &[f32],
) {
    append_rows(addr, &unit_vectors(280, DIM, 0x7A11_0003), 24, "ca9").await;
    flush_node(addr).await;
    append_rows(addr, &unit_vectors(16, DIM, 0x7A11_0004), 304, "scotus").await;

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let req = QueryRequest {
            k: 5,
            selection: Some(dense_leaf(qvec, Mode::Balanced, Precision::Auto)),
            ..Default::default()
        };
        let mixed = run_query(&coord, req).await.dense_execution.unwrap();
        assert_eq!(mixed.termination, DenseTermination::Converged as i32);
        assert_eq!(
            mixed.techniques,
            vec![
                tech_count(DenseTechnique::HnswWalk, 1),
                tech_count(DenseTechnique::Fp32Scan, 1),
            ]
        );
        assert_zero_hit_exhaustive_fabricates_no_parts(&coord, qvec).await;
    }
}

async fn assert_zero_hit_exhaustive_fabricates_no_parts(
    coordinator: &CoordinatorServiceImpl,
    qvec: &[f32],
) {
    let comp = CompositeSearchStrategy {
        operator: SelectionOperator::And as i32,
        clauses: vec![
            common::cel_selection("none", "court == 'missing'"),
            dense_leaf(qvec, Mode::Exhaustive, Precision::Auto),
        ],
        scoring: None,
    };
    let req = QueryRequest {
        k: 5,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(comp)),
        }),
        ..Default::default()
    };
    let zero = run_query(coordinator, req).await;
    assert!(zero.hits.is_empty());
    let out = zero.dense_execution.unwrap();
    assert_eq!(out.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(out.techniques.is_empty(), "{:?}", out.techniques);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_segment_composite_threads_real_shard_execution_evidence() {
    let dir = DirGuard::new("composite_evidence");
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let index = HnswIndex::new(DIM, HnswConfig::default())
            .unwrap()
            .into_index();
        let path = dir.0.join(format!("shard{shard}.idx"));
        let (addr, handle) =
            start_node(index, hnsw_segmented_config(path, (shard * 300) as u64)).await;
        append_rows(
            &addr,
            &unit_vectors(280, DIM, 0xC047_0010 + shard as u64),
            shard * 300,
            "ca9",
        )
        .await;
        flush_node(&addr).await;
        append_rows(
            &addr,
            &unit_vectors(20, DIM, 0xC047_0020 + shard as u64),
            shard * 300 + 280,
            "scotus",
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(3);
    let qvec = unit_vectors(1, DIM, 0xC047_0099);

    for s in [
        strat::Strategy::Rrf(RrfScore::default()),
        strat::Strategy::ScoreBlend(BlendScore::default()),
        strat::Strategy::Decomposed(DecomposedScore::default()),
        strat::Strategy::Cascade(CascadeScore {
            gate_id: "dense".into(),
        }),
    ] {
        let bal_req = composite_query(&qvec, Mode::Balanced, s.clone());
        let bal = run_query(&coordinator, bal_req)
            .await
            .dense_execution
            .unwrap();
        assert_eq!(bal.tail_enumerated_rows, 40);
        let want_bal = vec![
            tech_count(DenseTechnique::HnswWalk, 2),
            tech_count(DenseTechnique::Fp32Scan, 2),
        ];
        assert_eq!(bal.techniques, want_bal);
        let exh_req = composite_query(&qvec, Mode::Exhaustive, s);
        let exh = run_query(&coordinator, exh_req)
            .await
            .dense_execution
            .unwrap();
        assert_eq!(exh.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_eq!(
            exh.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 4)]
        );
    }
    for h in handles {
        h.abort();
    }
}
