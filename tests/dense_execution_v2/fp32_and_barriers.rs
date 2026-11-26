//! Shard-local FP32 sidecar scanning, admitted live-row verification, and
//! direct RPC / `SearchShard` knob guards.

use super::*;
use pipestream_search::analyzer::analyze_document_native;
use pipestream_search::exact_vectors::ExactVectorStore;
use pipestream_search::harness::serve_node;
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::node::{Bm25Shard, NodeServiceImpl};
use pipestream_search::pb::{search_shard_request, SearchShardRequest, StartShardSearch};
use pipestream_search::postings::Bm25Store;
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segmented_vectors::SegmentedProvider;
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource};
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhaustive_fp32_above_max_k_scans_shard_sidecars_and_direct_rpcs_guard_knobs() {
    let rows_per_shard = 128usize;
    let mut corpus = unit_vectors(rows_per_shard * 2, DIM, 0xF932_0001);
    let tie_row = corpus[0..DIM].to_vec();
    corpus[rows_per_shard * DIM..(rows_per_shard + 1) * DIM].copy_from_slice(&tie_row);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let cfg = NodeConfig {
            slot_offset: (shard * rows_per_shard) as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            facet_fields: vec!["court".into()],
            ..Default::default()
        };
        let (addr, handle) = start_node(seeded_index(DIM, 4, &shift, &scale), cfg).await;
        let slice = &corpus[shard * rows_per_shard * DIM..(shard + 1) * rows_per_shard * DIM];
        append_rows(&addr, slice, shard * rows_per_shard, "ca9").await;
        addrs.push(addr);
        handles.push(handle);
    }
    let bm25_addr = Some(NATIVE_ANALYSIS_BACKEND.to_string());
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(bm25_addr, Default::default())
        .with_max_k(16)
        .with_topology_generation(5);

    assert_exhaustive_fp32_matches_oracle(&coordinator, &corpus, &tie_row).await;
    assert_direct_rpc_knob_barriers(&coordinator, &tie_row).await;
    assert_half_patience_refused(&coordinator, &tie_row).await;
    assert_search_shard_collapse_messages(&addrs[0], &tie_row).await;
    for h in handles {
        h.abort();
    }
}

async fn assert_exhaustive_fp32_matches_oracle(
    coordinator: &CoordinatorServiceImpl,
    corpus: &[f32],
    qvec: &[f32],
) {
    let mut oracle: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| (id as u64, dot(row, qvec)))
        .collect();
    oracle.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    oracle.truncate(10);

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let resp = run_query(
            &coord,
            QueryRequest {
                k: 10,
                profile: true,
                selection: Some(dense_leaf(qvec, Mode::Exhaustive, Precision::Fp32)),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(resp.executed, "search:fp32_scan");
        assert_eq!(resp.profile.unwrap().rerank_rows, 256);
        let got: Vec<(u64, u32)> = resp
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect();
        let want: Vec<(u64, u32)> = oracle.iter().map(|&(id, s)| (id, s.to_bits())).collect();
        assert_eq!(got, want, "streaming={streaming}");
        let out = resp.dense_execution.unwrap();
        assert_eq!(out.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_eq!(out.output_precision, DenseScorePrecision::Fp32 as i32);
        assert_eq!(
            out.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 2)]
        );
    }
}

async fn assert_direct_rpc_knob_barriers(coordinator: &CoordinatorServiceImpl, qvec: &[f32]) {
    let bad_search = SearchRequest {
        k: 5,
        vector: qvec.to_vec(),
        enumerate_all: true,
        filter_strategy: DenseFilterStrategy::Speed as i32,
        ..Default::default()
    };
    let err = coordinator
        .search(Request::new(bad_search))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let unsupported_enum = SearchRequest {
        k: 5,
        vector: qvec.to_vec(),
        enumerate_all: true,
        ..Default::default()
    };
    let err = coordinator
        .search(Request::new(unsupported_enum))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let bad_hybrid = HybridSearchRequest {
        k: 5,
        text: "appellate".into(),
        vector: qvec.to_vec(),
        enumerate_all: true,
        patience_hops: Some(4),
        patience_saturation_ppm: Some(990_000),
        ..Default::default()
    };
    let err = coordinator
        .hybrid_search(Request::new(bad_hybrid))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let unsupported_hybrid = HybridSearchRequest {
        k: 5,
        text: "appellate".into(),
        vector: qvec.to_vec(),
        filter_strategy: DenseFilterStrategy::Speed as i32,
        ..Default::default()
    };
    let err = coordinator
        .hybrid_search(Request::new(unsupported_hybrid))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// Half-specified patience is refused on direct `Search` and `HybridSearch`
/// before any capability check, whichever half is missing.
async fn assert_half_patience_refused(coordinator: &CoordinatorServiceImpl, qvec: &[f32]) {
    const MESSAGE: &str = "patience_saturation_ppm and patience_hops must be set together";
    for (saturation, hops) in [(Some(990_000), None), (None, Some(15))] {
        let search = SearchRequest {
            k: 5,
            vector: qvec.to_vec(),
            patience_saturation_ppm: saturation,
            patience_hops: hops,
            ..Default::default()
        };
        let hybrid = HybridSearchRequest {
            k: 5,
            text: "appellate".into(),
            vector: qvec.to_vec(),
            patience_saturation_ppm: saturation,
            patience_hops: hops,
            ..Default::default()
        };
        let refusals = [
            coordinator.search(Request::new(search)).await.unwrap_err(),
            coordinator
                .hybrid_search(Request::new(hybrid))
                .await
                .unwrap_err(),
        ];
        for err in refusals {
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{}",
                err.message()
            );
            assert!(err.message().contains(MESSAGE), "{}", err.message());
        }
    }
}

async fn assert_search_shard_collapse_messages(addr: &str, qvec: &[f32]) {
    for (exact_fp32_scan, tie_complete, expected) in [
        (
            true,
            false,
            "collapse_parents and exact_fp32_scan are mutually exclusive",
        ),
        (
            false,
            true,
            "collapse_parents and tie_complete are mutually exclusive",
        ),
    ] {
        let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
        let req = SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(StartShardSearch {
                request_id: "collapse-exclusivity".into(),
                k: 5,
                vector: qvec.to_vec(),
                collapse_parents: true,
                exact_fp32_scan,
                tie_complete,
                ..Default::default()
            })),
        };
        let mut stream = client
            .search_shard(tokio_stream::iter(vec![req]))
            .await
            .unwrap()
            .into_inner();
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), expected);
    }
}

fn build_gap_catalog(
    root: &std::path::Path,
    vectors: &[f32],
    shift: &[f32],
    scale: &[f32],
) -> std::sync::Arc<OpenedSegmentSet> {
    let catalog = SegmentCatalog::open(root.join("catalog")).unwrap();
    for part in 0..2usize {
        let stage = root.join(format!("stage-{part}"));
        std::fs::create_dir_all(&stage).unwrap();
        let bm25 = stage.join("documents.bm25");
        let live = stage.join("live.bin");
        let image = stage.join("vector.index");
        let exact = stage.join("vectors.f32");
        let mut store = Bm25Store::with_text_fields(&["body"]);
        for slot in 0..4u32 {
            store.add_document(
                slot,
                "opinion".into(),
                analyze_document_native("opinion", Some(&body_spec())).unwrap(),
            );
        }
        store.save(&bm25).unwrap();
        LiveDocs::default().write(&live, 4).unwrap();
        if part == 1 {
            let slice = &vectors[4 * DIM..8 * DIM];
            let mut idx = seeded_index(DIM, 4, shift, scale);
            idx.add(slice, DIM).unwrap();
            idx.prepare().unwrap();
            idx.write(&image).unwrap();
            ExactVectorStore::from_values(DIM, slice.to_vec())
                .unwrap()
                .write(&exact)
                .unwrap();
        }
        catalog
            .append(SegmentSource {
                multi_res_path: None,
                segment_id: &format!("part-{part}"),
                generation: part as u64 + 1,
                base_label: (part * 4) as u64,
                backend_kind: if part == 0 { "" } else { EMBEDDED_TURBOVEC },
                vector_path: (part == 1).then_some(image.as_path()),
                exact_vector_path: (part == 1).then_some(exact.as_path()),
                bm25_path: &bm25,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
    }
    std::sync::Arc::new(OpenedSegmentSet::open(root.join("catalog")).unwrap())
}

async fn assert_gap_shard_succeeds(valid_addr: &str, qvec: &[f32]) {
    for streaming in [false, true] {
        let coord = CoordinatorServiceImpl::new(vec![valid_addr.to_string()])
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let resp = run_query(
            &coord,
            QueryRequest {
                k: 4,
                profile: true,
                selection: Some(dense_leaf(qvec, Mode::Exhaustive, Precision::Fp32)),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(resp.hits.len(), 4);
        assert_eq!(resp.profile.unwrap().rerank_rows, 4);
        assert_eq!(
            resp.dense_execution.unwrap().coverage,
            DenseCoverage::ExhaustiveCertified as i32
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhaustive_fp32_handles_doc_only_gaps_and_refuses_missing_live_vector_rows() {
    let dir = DirGuard::new("fp32_admitted_rows");
    let vectors = unit_vectors(16, DIM, 0xF932_7701);
    let qvec = vectors[4 * DIM..5 * DIM].to_vec();
    let (shift, scale) = fit_calibration(DIM, 4, &vectors);
    let set = build_gap_catalog(&dir.0, &vectors[..8 * DIM], &shift, &scale);

    let seg_provider =
        SegmentedProvider::open(set.clone(), seeded_index(DIM, 4, &shift, &scale)).unwrap();
    let seg_bm25 = SegmentedShard::open(
        dir.0.join("catalog"),
        Bm25Store::with_text_fields(&["body"]),
    )
    .unwrap();
    let valid_node = NodeServiceImpl::new(
        Some(VectorIndex::from_provider(seg_provider)),
        NodeConfig::default(),
    )
    .with_bm25(Some(Bm25Shard::Segmented(seg_bm25)))
    .with_exact_vectors(Some(ExactVectorStore::from_segments(&set, DIM).unwrap()))
    .unwrap();
    let (valid_addr, valid_h) = serve_node(valid_node).await;
    assert_gap_shard_succeeds(&valid_addr, &qvec).await;

    let mut full_idx = seeded_index(DIM, 4, &shift, &scale);
    full_idx.add(&vectors[8 * DIM..16 * DIM], DIM).unwrap();
    full_idx.prepare().unwrap();
    let short_node = NodeServiceImpl::new(
        Some(full_idx),
        NodeConfig {
            slot_offset: 100,
            ..Default::default()
        },
    )
    .with_exact_vectors(Some(ExactVectorStore::from_segments(&set, DIM).unwrap()))
    .unwrap();
    let (short_addr, short_h) = serve_node(short_node).await;

    for streaming in [false, true] {
        let coord = CoordinatorServiceImpl::new(vec![valid_addr.clone(), short_addr.clone()])
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let err = coord
            .query(Request::new(QueryRequest {
                k: 4,
                selection: Some(dense_leaf(&qvec, Mode::Exhaustive, Precision::Fp32)),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert!(
            err.message()
                .contains("missing admitted live vector row 100"),
            "streaming={streaming}: {err}"
        );
    }
    valid_h.abort();
    short_h.abort();
}
