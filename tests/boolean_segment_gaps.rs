//! Membership must use vector images' positional ranges, including gaps.
use pipestream_search::analyzer::{analyze_document_native, body_spec};
use pipestream_search::harness::{fit_calibration, seeded_index, unit_vectors};
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::node::{Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::pb::*;
use pipestream_search::postings::Bm25Store;
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segmented_vectors::SegmentedProvider;
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource};
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
use tonic::Request;

fn documents() -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body"]);
    for slot in 0..2 {
        store.add_document(
            slot,
            "word".into(),
            analyze_document_native("word", Some(&body_spec())).unwrap(),
        );
    }
    store
}

#[tokio::test]
async fn document_only_segment_is_a_gap_in_bitmap_and_boolean_membership() {
    let root = std::env::temp_dir().join(format!("boolean-segment-gaps-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let vectors = unit_vectors(8, 16, 731);
    let (shift, scale) = fit_calibration(16, 4, &vectors);
    let catalog = SegmentCatalog::open(root.join("catalog")).unwrap();
    for part in 0..3 {
        let stage = root.join(format!("stage-{part}"));
        std::fs::create_dir_all(&stage).unwrap();
        let bm25 = stage.join("documents.bm25");
        let live = stage.join("live.bin");
        let image = stage.join("vector.index");
        let exact = stage.join("vectors.f32");
        documents().save(&bm25).unwrap();
        LiveDocs::default().write(&live, 2).unwrap();
        if part != 1 {
            let mut index = seeded_index(16, 4, &shift, &scale);
            index.add(&vectors[part * 32..(part + 1) * 32], 16).unwrap();
            index.prepare().unwrap();
            index.write(&image).unwrap();
            pipestream_search::exact_vectors::ExactVectorStore::from_values(
                16,
                vectors[part * 32..(part + 1) * 32].to_vec(),
            )
            .unwrap()
            .write(&exact)
            .unwrap();
        }
        catalog
            .append(SegmentSource {
                segment_id: &format!("part-{part}"),
                generation: part as u64 + 1,
                base_label: (part * 2) as u64,
                backend_kind: if part == 1 { "" } else { EMBEDDED_TURBOVEC },
                vector_path: (part != 1).then_some(image.as_path()),
                exact_vector_path: (part != 1).then_some(exact.as_path()),
                bm25_path: &bm25,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
    }
    let set = std::sync::Arc::new(OpenedSegmentSet::open(root.join("catalog")).unwrap());
    let provider = SegmentedProvider::open(set, seeded_index(16, 4, &shift, &scale)).unwrap();
    let mut index = VectorIndex::from_provider(provider);
    index.add(&vectors[96..], 16).unwrap();
    index.prepare().unwrap();
    assert_eq!(index.len(), 8);
    assert_eq!(index.vector_rows(), vec![(0, 2), (4, 2), (6, 2)]);
    let mut bm25 =
        SegmentedShard::open(root.join("catalog"), Bm25Store::with_fields(&["body"])).unwrap();
    for slot in 6..8 {
        bm25.add_document(
            slot,
            "word".into(),
            analyze_document_native("word", Some(&body_spec())).unwrap(),
            None,
        )
        .unwrap();
    }
    let node = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            slot_offset: 100,
            ..Default::default()
        },
    )
    .with_bm25(Some(Bm25Shard::Segmented(bm25)));
    let bitmap = node
        .resolve_vector_bitmap(Request::new(VectorBitmapRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((bitmap.base_label, bitmap.label_count), (100, 8));
    assert_eq!(bitmap.bits, vec![0b1111_0011]);
    let leaf = |i| BooleanPlanNode {
        node: Some(boolean_plan_node::Node::Leaf(i)),
    };
    let leaves = vec![
        BooleanPlanLeaf {
            leaf: Some(boolean_plan_leaf::Leaf::Dense(BooleanPlanDense {
                vector: vectors[..16].to_vec(),
                ..Default::default()
            })),
        },
        BooleanPlanLeaf {
            leaf: Some(boolean_plan_leaf::Leaf::Filter(BooleanPlanFilter::default())),
        },
    ];
    let groups = [
        (
            BooleanPlanGroup {
                must: vec![leaf(0)],
                ..Default::default()
            },
            vec![100, 101, 104, 105, 106, 107],
        ),
        (
            BooleanPlanGroup {
                must: vec![leaf(1)],
                should: vec![leaf(0)],
                ..Default::default()
            },
            (100..108).collect(),
        ),
        (
            BooleanPlanGroup {
                must: vec![leaf(1)],
                must_not: vec![leaf(0)],
                ..Default::default()
            },
            vec![102, 103],
        ),
        (
            BooleanPlanGroup {
                must_not: vec![leaf(0)],
                ..Default::default()
            },
            vec![102, 103],
        ),
    ];
    for (group, expected) in groups {
        let response = node
            .evaluate_boolean(Request::new(BooleanShardRequest {
                root: Some(group),
                leaves: leaves.clone(),
                depth: 8,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut ids: Vec<_> = response.candidates.iter().map(|c| c.doc_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, expected);
        for candidate in response
            .candidates
            .iter()
            .filter(|c| [102, 103].contains(&c.doc_id))
        {
            assert!(candidate.signals.is_empty());
            assert!(!candidate.matched.contains(&0));
        }
    }
    // Missing rerank storage cannot silently redefine dense membership.
    let mut exact_leaves = leaves;
    let Some(boolean_plan_leaf::Leaf::Dense(dense)) = exact_leaves[0].leaf.as_mut() else {
        unreachable!()
    };
    dense.exact_fp32 = true;
    let error = node
        .evaluate_boolean(Request::new(BooleanShardRequest {
            root: Some(BooleanPlanGroup {
                must: vec![leaf(0)],
                ..Default::default()
            }),
            leaves: exact_leaves,
            depth: 8,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("exact-vector sidecar"));
    drop(node);
    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}
