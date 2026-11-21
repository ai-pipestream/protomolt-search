//! Coalesced catch-up streams: unkeyed and keyed runs, their caps, and a
//! retry that finds every row already present.

use crate::pair::{appended, document, key, Pair};
use pipestream_search::pb::wal::wal_record::Op;

#[tokio::test]
async fn unkeyed_document_records_share_streams_and_a_retry_sends_nothing() {
    let mut pair = Pair::start("unkeyed", false).await;
    pair.documents(0..530, None).await;
    let start = pair.cursor.clone();
    let advanced = pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddDocuments", "AddDocuments"]);
    // A cursor lost after the replica flushed: every row is already there.
    assert!(pair.sync(&start).await.clock >= advanced.clock);
    assert_eq!(pair.appends().len(), 2);
    pair.documents(530..540, None).await;
    assert!(pair.sync(&start).await.clock > advanced.clock);
    assert_eq!(pair.appends().len(), 3);
}

#[tokio::test]
async fn keyed_rows_share_one_stream_and_keep_their_key_between_unkeyed_runs() {
    let mut pair = Pair::start("keyed", false).await;
    pair.documents(0..3, None).await;
    pair.documents(3..5, Some(b"stable-key")).await;
    pair.documents(5..8, None).await;
    let start = pair.cursor.clone();
    pair.sync(&start).await;
    assert_eq!(
        pair.appends().len(),
        3,
        "one unkeyed run, one keyed run, one unkeyed run"
    );
    let keys: Vec<Vec<Vec<u8>>> = appended(&pair.cursor.replica)
        .await
        .into_iter()
        .map(|op| match op {
            Op::AddDocuments(add) => add.stable_routing_keys,
            other => panic!("unexpected record {other:?}"),
        })
        .collect();
    let keyed = vec![b"stable-key".to_vec()];
    let expected = [
        vec![],
        vec![],
        vec![],
        keyed.clone(),
        keyed,
        vec![],
        vec![],
        vec![],
    ];
    assert_eq!(keys, expected);
}

#[tokio::test]
async fn over_five_hundred_keyed_rows_replicate_in_two_streams_under_their_own_keys() {
    const ROWS: usize = 530;
    let mut pair = Pair::start("keyed-batch", false).await;
    let keys: Vec<Vec<u8>> = (0..ROWS).map(key).collect();
    pair.keyed((0..ROWS).map(document).collect(), &keys).await;
    let start = pair.cursor.clone();
    let advanced = pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddDocuments", "AddDocuments"]);
    let replicated = appended(&pair.cursor.replica).await;
    assert_eq!(replicated.len(), ROWS);
    for (row, op) in replicated.into_iter().enumerate() {
        let Op::AddDocuments(add) = op else {
            panic!("row {row} is not a document record")
        };
        assert_eq!(add.first_id, row as u64);
        assert_eq!(add.stable_routing_keys, [key(row)], "row {row}");
        assert_eq!(add.documents.len(), 1, "row {row}");
        assert_eq!(add.documents[0].text, document(row).text, "row {row}");
    }
    // A cursor lost after the replica flushed sends nothing again.
    assert!(pair.sync(&start).await.clock >= advanced.clock);
    assert_eq!(pair.appends().len(), 2);
    pair.keyed(vec![document(ROWS)], &[key(ROWS)]).await;
    assert!(pair.sync(&advanced).await.clock > advanced.clock);
    assert_eq!(pair.appends().len(), 3);
}

#[tokio::test]
async fn unkeyed_vector_records_share_one_stream_and_keep_their_records() {
    let mut pair = Pair::start("vectors", true).await;
    for seed in 0..3 {
        pair.vectors(4, 0xB17_0100 + seed, None).await;
    }
    let start = pair.cursor.clone();
    let advanced = pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddVectors"]);
    pair.vectors(2, 0xB17_0200, None).await;
    assert!(pair.sync(&start).await.clock > advanced.clock);
    assert_eq!(pair.appends(), ["AddVectors", "AddVectors"]);
}

#[tokio::test]
async fn keyed_vector_rows_share_a_stream_apart_from_unkeyed_ones() {
    let mut pair = Pair::start("keyed-vectors", true).await;
    pair.vectors(3, 0xB17_0300, None).await;
    for row in 0..4 {
        pair.vectors(1, 0xB17_0400 + row, Some(&key(row as usize)))
            .await;
    }
    pair.vectors(2, 0xB17_0500, None).await;
    let start = pair.cursor.clone();
    pair.sync(&start).await;
    assert_eq!(pair.appends(), ["AddVectors", "AddVectors", "AddVectors"]);
    let keys: Vec<Vec<Vec<u8>>> = appended(&pair.cursor.replica)
        .await
        .into_iter()
        .map(|op| match op {
            Op::AddVectors(add) => add.stable_routing_keys,
            other => panic!("unexpected record {other:?}"),
        })
        .collect();
    // The primary logs one record per vector row.
    let expected: Vec<Vec<Vec<u8>>> = std::iter::repeat_n(vec![], 3)
        .chain((0..4).map(|row| vec![key(row)]))
        .chain(std::iter::repeat_n(vec![], 2))
        .collect();
    assert_eq!(keys, expected);
}

#[tokio::test]
async fn an_empty_keyed_vector_batch_is_refused_without_consuming_the_next_key() {
    let pair = Pair::start("empty-keyed-vectors", true).await;
    let empty = pipestream_search::pb::AddVectorsRequest {
        dim: crate::pair::DIM as u32,
        ..Default::default()
    };
    let valid = pipestream_search::pb::AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: crate::common::unit_vectors(1, crate::pair::DIM, 0xB17_0600),
        dim: crate::pair::DIM as u32,
    };
    let mut request = tonic::Request::new(tokio_stream::iter([empty, valid]));
    let keys = pipestream_search::replication::encode_stable_keys(&[key(0), key(1)]).unwrap();
    request.metadata_mut().insert_bin(
        pipestream_search::replication::STABLE_KEYS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&keys),
    );
    let mut client = pair.source.clone();
    let error = client.add_vectors(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
    assert!(error.message().contains("at least one vector"), "{error}");
    let health = client
        .health(pipestream_search::pb::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        health.num_vectors, 0,
        "the later keyed row was not consumed"
    );
}
