//! Post-reshard keyed appends, the parent-id collision, vectors-only.

use super::*;

/// The child a parent's key hash names under the online routing rule:
/// the child whose hash range holds the key's stable routing hash.
fn online_child(output: &reshard::ReshardOutput, key: &str) -> usize {
    let hash = pipestream_search::coordinator::stable_routing_hash(key.as_bytes());
    output
        .children
        .iter()
        .position(|image| image.hash_lo <= hash && hash <= image.hash_hi)
        .expect("some child owns every hash")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_reshard_keyed_appends_land_on_the_resharded_home() {
    let dir = tempdir("post-reshard");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    // Every parent's resharded home is the child its stable key routes
    // to online, so a later keyed chunk cannot scatter the parent.
    let mut homes: BTreeMap<u64, usize> = BTreeMap::new();
    for (child, image) in output.children.iter().enumerate() {
        if image.num_documents == 0 {
            continue;
        }
        for parent in parents_of(&catalog(&image.vector_path)) {
            if parent != u64::MAX {
                homes.insert(parent, child);
            }
        }
    }
    assert_eq!(homes.len() as u64, PARENTS);
    for p in 0..PARENTS {
        assert_eq!(
            homes[&parent_id(p)],
            online_child(&output, &format!("opinion-{p}")),
            "parent {p} would scatter on its next keyed append"
        );
    }

    // Serve parent 0's child and append one more keyed chunk of it: the
    // new segment keeps the proof and the parent stays whole there.
    let home = &output.children[homes[&parent_id(0)]];
    let (addr, handle) = common::start_opened_node(config(&home.vector_path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    append(
        &mut client,
        "opinion 0 late chunk court".into(),
        Some(parent_id(0)),
        Some("opinion-0".into()),
    )
    .await;
    client.flush(FlushRequest {}).await.unwrap();
    let set = catalog(&home.vector_path);
    assert!(set.parents_colocated(), "the late chunk keeps the proof");
    assert_eq!(
        parents_of(&set)
            .into_iter()
            .filter(|&parent| parent == parent_id(0))
            .count() as u64,
        CHUNKS + 1
    );
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_chunk_at_its_parent_id_routes_by_key_not_bucket() {
    // The only lineage row in the log sits at the global id its parent
    // id names. That coincidence must not let the cheap bucket-range
    // split claim the proof: opinion-5 hashes to child 3 online, while
    // row 5's WAL bucket belongs to child 0.
    let dir = tempdir("single-chunk");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 0xC0_10CA7E));
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            8, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    for row in 0..5 {
        append(
            &mut client,
            format!("lone row {row} court"),
            None,
            Some(format!("lone-{row}")),
        )
        .await;
    }
    append(
        &mut client,
        "opinion 5 only chunk court".into(),
        Some(5),
        Some("opinion-5".into()),
    )
    .await;
    client.flush(FlushRequest {}).await.unwrap();
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    let home = output
        .children
        .iter()
        .position(|image| {
            image.num_documents > 0 && parents_of(&catalog(&image.vector_path)).contains(&5)
        })
        .expect("the single chunk lands somewhere");
    assert_eq!(home, online_child(&output, "opinion-5"));
    assert!(output.children[home].parents_colocated);
    assert!(catalog(&output.children[home].vector_path).parents_colocated());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vectors_only_split_seals_no_colocation_proof() {
    let dir = tempdir("vectors-only");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    // Aligned parent: every document first, then every vector, one
    // flush at the end so no documents-only segment ever seals.
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 0xC0_10CA7E));
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            8, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    for row in 0..LONE_ROWS {
        append(
            &mut client,
            format!("lone row {row} court"),
            None,
            Some(format!("lone-{row}")),
        )
        .await;
        if row < PARENTS * CHUNKS {
            let parent = row % PARENTS;
            append(
                &mut client,
                format!("opinion {parent} chunk {row} court"),
                Some(parent_id(parent)),
                Some(format!("opinion-{parent}")),
            )
            .await;
        }
    }
    let rows = (LONE_ROWS + PARENTS * CHUNKS) as usize;
    let corpus = common::unit_vectors(rows, 8, 0xC0_10CA7E);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        tx.send(pipestream_search::pb::AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus,
            dim: 8,
        })
        .await
        .unwrap();
    });
    assert_eq!(
        client
            .add_vectors(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner()
            .added as usize,
        rows
    );
    client.flush(FlushRequest {}).await.unwrap();
    handle.abort();

    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path)).unwrap();
    let out_dir = dir.join("split");
    let request = reshard::SplitRequest::new(std::slice::from_ref(&gen), 4, &out_dir)
        .slot_stride(1_000)
        .vectors_only(true);
    let output = reshard::split_logs_with(&request, &mut native_analyzer).unwrap();
    let mut sealed_any = false;
    for image in output.children.iter().filter(|i| i.num_vectors > 0) {
        assert!(
            !image.parents_colocated,
            "a vectors-only child cannot claim lineage proof it never replayed"
        );
        let set = catalog(&image.vector_path);
        if !set.is_empty() {
            sealed_any = true;
            assert!(!set.parents_colocated());
        }
    }
    assert!(sealed_any, "the split sealed at least one vector segment");
    let _ = std::fs::remove_dir_all(&dir);
}
