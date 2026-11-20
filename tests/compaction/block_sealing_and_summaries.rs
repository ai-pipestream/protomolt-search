//! Lockstep vector/document block sealing, un-flushed reopen sharing, and seal summaries.

use super::*;

/// The batched form of the legacy append: one AddDocuments stream for
/// `rows`, then one AddVectors stream for the same rows, unkeyed (ids
/// and slots align by order within the block).
async fn add_block(client: &mut NodeServiceClient<Channel>, rows: &[Row]) {
    let docs: Vec<AddDocumentsRequest> = rows
        .iter()
        .map(|row| AddDocumentsRequest {
            text: row.text.clone(),
            lineage: Some(DocLineage {
                parent_id: row.num as u64,
                ..Default::default()
            }),
            facets: vec![FacetValue {
                field: "grp".into(),
                value: row.grp.to_string(),
            }],
            integers: vec![IntegerValue {
                field: "num".into(),
                value: row.num,
            }],
            ..Default::default()
        })
        .collect();
    let added = client
        .add_documents(tokio_stream::iter(docs))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added as usize, rows.len());
    let vectors: Vec<f32> = rows.iter().flat_map(|row| row.vector.clone()).collect();
    let added = client
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added as usize, rows.len());
}

/// The rebuild driver's order on the segment layout: the backend is
/// configured first, then documents and vectors arrive in blocks. The
/// seal bound lands mid-block, so the tail must wait for the block's
/// vectors before sealing; every sealed segment then carries its
/// vectors and FP32 rows, and the flush succeeds. Every document first,
/// then every vector, seals document-only segments the vectors can never
/// join, which the first vector batch refuses by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocks_seal_with_their_vectors_and_documents_first_is_refused_by_name() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("blocks");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let rows: Vec<Row> = (0..320).map(|i| row(i, false)).collect();

    // Blocks of 64 with a seal bound of 100: the bound falls inside the
    // second block (128 documents against 64 vectors).
    let index_path = dir.join("blocks.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut blocks = client(&addr).await;
    for block in rows.chunks(64) {
        add_block(&mut blocks, block).await;
    }
    let health = blocks.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.num_vectors, 320);
    assert_eq!(health.bm25_docs, 320);
    let root = pipestream_search::node::segments_root(&index_path);
    let set = pipestream_search::segments::OpenedSegmentSet::open(&root).unwrap();
    assert!(
        set.len() >= 2,
        "the bound sealed segments during ingest: {}",
        set.len()
    );
    let mut sealed_rows = 0usize;
    for i in 0..set.len() {
        let image = set
            .vector(i)
            .unwrap_or_else(|| panic!("segment {i} sealed without its vectors"));
        assert_eq!(
            image.len(),
            set.metadata(i).rows as usize,
            "segment {i} rows carry vectors"
        );
        sealed_rows += set.metadata(i).rows as usize;
    }
    assert!(
        sealed_rows >= 200 && sealed_rows.is_multiple_of(64),
        "sealed at block edges: {sealed_rows}"
    );
    let flushed = blocks.flush(FlushRequest {}).await.unwrap().into_inner();
    assert_eq!(flushed.num_vectors, 320);
    assert_eq!(flushed.num_documents, 320);
    let exact = pipestream_search::exact_vectors::ExactVectorStore::open(
        &pipestream_search::node::exact_vector_sidecar_path(&index_path),
    );
    if let Ok(exact) = exact {
        assert_eq!(exact.len(), 320);
        exact.verify_payload().unwrap();
    }
    handle.abort();

    // Documents first, every one of them: the tail waits for their
    // vectors rather than sealing without them, whatever the bound. A
    // flush seals what the tail has, documents only, and the vectors
    // that arrive after that have no segment to join.
    let other_path = dir.join("docs-first.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(other_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut docs_first = client(&addr).await;
    for r in &rows[..250] {
        add_document(&mut docs_first, r).await;
    }
    let root = pipestream_search::node::segments_root(&other_path);
    assert!(
        pipestream_search::segments::OpenedSegmentSet::open(&root)
            .map_or(true, |set| set.is_empty()),
        "no document-only segment sealed on its own"
    );
    // The flush seals the document-only segment; its rows have no FP32
    // rows, and the exact view over the catalog says so.
    let flushed = docs_first
        .flush(FlushRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(flushed.num_documents, 250);
    let vectors: Vec<f32> = rows[..250].iter().flat_map(|r| r.vector.clone()).collect();
    let status = docs_first
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        }]))
        .await
        .expect_err("vectors after document-only seals");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        status.message()
    );
    assert!(
        status
            .message()
            .contains("sealed in segments without vectors"),
        "{}",
        status.message()
    );
    handle.abort();
}

/// A node that stopped without a flush (a refused shutdown flush, a
/// crash) never finalized its whole-shard FP32 sidecar, but the sealed
/// rows' FP32 files live in the segments: the next open shares them without
/// writing a whole-shard sidecar, and appends continue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_reopened_without_a_flush_shares_its_exact_segment_images() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("reopen");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let rows: Vec<Row> = (0..256).map(|i| row(i, false)).collect();
    let index_path = dir.join("reopen.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 64,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut first = client(&addr).await;
    for block in rows.chunks(64) {
        add_block(&mut first, block).await;
    }
    // Every block sealed (the bound equals the block); no flush ran.
    handle.abort();
    let exact_path = pipestream_search::node::exact_vector_sidecar_path(&index_path);
    assert!(!exact_path.exists(), "no flush, no finalized sidecar");

    let (addr, handle) = start_opened_node(config(Some(index_path.clone()), &analysis)).await;
    assert!(
        !exact_path.exists(),
        "reopening must not copy the full FP32 corpus"
    );
    let set = pipestream_search::segments::OpenedSegmentSet::open(
        pipestream_search::node::segments_root(&index_path),
    )
    .unwrap();
    let exact =
        pipestream_search::exact_vectors::ExactVectorStore::from_segments(&set, DIM).unwrap();
    assert_eq!(exact.len(), 256);
    exact.verify_payload().unwrap();
    assert_eq!(exact.row_values(5, 6).unwrap(), rows[5].vector);
    assert_eq!(exact.row_values(200, 201).unwrap(), rows[200].vector);

    let mut second = client(&addr).await;
    let more: Vec<Row> = (256..320).map(|i| row(i, false)).collect();
    add_block(&mut second, &more).await;
    let health = second.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.num_vectors, 320);
    assert_eq!(health.bm25_docs, 320);
    let flushed = second.flush(FlushRequest {}).await.unwrap().into_inner();
    assert_eq!(flushed.num_vectors, 320);
    assert!(
        !exact_path.exists(),
        "segment flush persists no redundant sidecar"
    );
    let set = pipestream_search::segments::OpenedSegmentSet::open(
        pipestream_search::node::segments_root(&index_path),
    )
    .unwrap();
    let exact =
        pipestream_search::exact_vectors::ExactVectorStore::from_segments(&set, DIM).unwrap();
    assert_eq!(exact.len(), 320);
    exact.verify_payload().unwrap();
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_seal_records_column_summaries() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("summaries");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, _handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 100,
        ..config(Some(index_path.clone()), &analysis)
    })
    .await;
    calibrate(&addr, &shift, &scale).await;
    let mut node = client(&addr).await;
    let rows = 250usize;
    for i in 0..rows {
        // Shuffled nums: (i * 37) mod 251 is a permutation of 0..251.
        let mut r = row(i, false);
        r.num = (i as i64 * 37) % 251;
        r.key = format!("shuffled/{i}");
        append(&mut node, &r).await;
    }
    node.flush(FlushRequest {}).await.unwrap();
    let manifest = read_manifest(&index_path);
    assert!(
        manifest.segments.len() >= 3,
        "{} segments",
        manifest.segments.len()
    );
    assert_eq!(manifest.partition_key, None);
    let mut present_total = 0;
    for segment in &manifest.segments {
        let summary = segment.summary.as_ref().expect("a seal writes a summary");
        assert!(summary.partition.is_none());
        let (min, max, present) = int_summary(segment, "num");
        assert_eq!(present, segment.rows);
        assert!(min <= max);
        assert!((0..251).contains(&min) && (0..251).contains(&max));
        present_total += present;
        // The range is the segment's own rows, not the shard's.
        let ids: Vec<u64> = (segment.base_label..segment.base_label + segment.rows).collect();
        let fetched = node
            .get_documents(GetDocumentsRequest { doc_ids: ids })
            .await
            .unwrap()
            .into_inner();
        let nums: Vec<i64> = fetched
            .documents
            .iter()
            .map(|d| {
                let n: i64 = d
                    .text
                    .trim_start_matches("row")
                    .split(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap();
                (n * 37) % 251
            })
            .collect();
        assert_eq!(nums.iter().min().copied(), Some(min));
        assert_eq!(nums.iter().max().copied(), Some(max));
        let (omin, omax, opresent) = int_summary(segment, "other");
        assert_eq!(opresent, 0);
        assert!(omin > omax, "an absent int column has an inverted range");
        let score = summary
            .numeric_columns
            .iter()
            .find(|c| c.name == "score")
            .expect("the double column is summarized");
        assert_eq!(score.present, 0);
        assert!(
            score.min > score.max,
            "an absent double column has an inverted range"
        );
    }
    assert_eq!(present_total, rows as u64);
}
