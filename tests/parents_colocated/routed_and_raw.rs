//! Routed parents stay whole with proof; raw lineage never earns it.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_parents_reshard_whole_and_keep_their_proof() {
    let dir = tempdir("routed");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    assert!(
        catalog(&path).parents_colocated(),
        "routed ingest proves it"
    );
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    for image in output.children.iter().filter(|i| i.num_documents > 0) {
        assert!(image.parents_colocated);
        assert!(catalog(&image.vector_path).parents_colocated());
        let child = NodeServiceImpl::open(config(&image.vector_path), None).unwrap();
        assert!(child.parents_colocated(), "a child node serves the proof");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The child a parent's key hash names under the online routing rule:
/// the child whose hash range holds the key's stable routing hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_lineage_cannot_claim_colocation_and_a_reshard_does_not_launder_it() {
    let dir = tempdir("raw");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, false).await;
    assert!(
        !catalog(&path).parents_colocated(),
        "raw lineage ingest proves nothing about other shards"
    );
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    for image in output.children.iter().filter(|i| i.num_documents > 0) {
        let set = catalog(&image.vector_path);
        let parented = parents_of(&set).iter().any(|&parent| parent != u64::MAX);
        assert_eq!(
            set.parents_colocated(),
            !parented,
            "only a child of unparented rows proves colocation"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
