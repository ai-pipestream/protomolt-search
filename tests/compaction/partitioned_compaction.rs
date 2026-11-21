//! Column-partitioned compaction, unvalued-row isolation, and wrong-column refusal.

use super::*;

// The partitioned layout ("Partitioned layout")
// and the column summaries a seal records ("Segment summaries").

fn partition_request(column: &str, bound: u32) -> CompactShardRequest {
    CompactShardRequest {
        partition_column: column.into(),
        tail_bound: bound,
        ..Default::default()
    }
}

/// The compaction outputs whose id starts with `prefix`, in base order:
/// each within `bound` rows, the keyed ones ascending and disjoint by
/// `num` with a partition range equal to the segment's own `num` range,
/// the unkeyed ones after them. Returns `(keyed, unkeyed)` segments.
fn check_partitions(
    manifest: &SegmentSetManifest,
    prefix: &str,
    bound: u64,
) -> (Vec<SegmentMetadata>, Vec<SegmentMetadata>) {
    assert_eq!(manifest.partition_key.as_deref(), Some("num"));
    let outputs: Vec<&SegmentMetadata> = manifest
        .segments
        .iter()
        .filter(|s| s.segment_id.starts_with(prefix))
        .collect();
    assert!(!outputs.is_empty(), "no outputs with prefix {prefix}");
    let mut keyed = Vec::new();
    let mut unkeyed = Vec::new();
    let mut previous_hi: Option<i64> = None;
    for segment in outputs {
        assert!(
            segment.rows <= bound,
            "segment {} holds {} rows over the bound {bound}",
            segment.segment_id,
            segment.rows
        );
        let (min, max, present) = int_summary(segment, "num");
        let partition = segment.summary.as_ref().unwrap().partition.as_ref();
        match partition {
            Some(range) => {
                assert!(unkeyed.is_empty(), "a keyed segment after an unkeyed one");
                assert_eq!(range.column, "num");
                assert_eq!((range.lo, range.hi), (min, max));
                assert_eq!(
                    present, segment.rows,
                    "a keyed segment holds keyed rows only"
                );
                if let Some(hi) = previous_hi {
                    assert!(
                        hi < range.lo,
                        "segment {} range {}..={} overlaps the previous hi {hi}",
                        segment.segment_id,
                        range.lo,
                        range.hi
                    );
                }
                previous_hi = Some(range.hi);
                keyed.push(segment.clone());
            }
            None => {
                assert_eq!(present, 0, "an unkeyed segment holds no keyed row");
                assert!(min > max, "an absent column has an inverted range");
                unkeyed.push(segment.clone());
            }
        }
    }
    (keyed, unkeyed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segmented_shard_compacts_into_num_partitions() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition");
    let sample = unit_vectors(256, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, mut handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    let tracked = seed_shard(&addr, &shift, &scale).await;
    let queries: Vec<Vec<f32>> = (0..3)
        .map(|q| unit_vectors(1, DIM, 0x5E61_0000 + q))
        .collect();
    let live_rows = tracked.live.len() as u64;

    let bound = 64u32;
    let first = compact(&addr, partition_request("num", bound))
        .await
        .unwrap();
    eprintln!("partitioned compaction #1: {first:?}");
    assert_eq!(first.partition_column, "num");
    assert_eq!(first.layout, "segments");
    assert_eq!(first.rows_after, live_rows);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    assert!(unkeyed.is_empty(), "every seeded row carries num");
    assert!(keyed.len() >= (live_rows / u64::from(bound)) as usize);
    assert_eq!(keyed.iter().map(|s| s.rows).sum::<u64>(), live_rows);
    // Each segment's rows are in ascending num order: the browse by id
    // over one segment's range returns ascending nums.
    {
        let mut node = client(&addr).await;
        let segment = &keyed[keyed.len() / 2];
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
                let text = &d.text;
                let n: usize = text
                    .trim_start_matches("row")
                    .split(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap();
                n as i64 * 2 + i64::from(text.ends_with("revised"))
            })
            .collect();
        let mut sorted = nums.clone();
        sorted.sort_unstable();
        assert_eq!(nums, sorted, "rows inside a segment are in key order");
        let (min, max, _) = int_summary(segment, "num");
        assert_eq!((nums[0], *nums.last().unwrap()), (min, max));
    }

    // Every read path equals a shard built fresh from the same rows.
    let final_rows: Vec<Row> = tracked.live.values().cloned().collect();
    let (reference, reference_handle) = reference_shard(
        &dir,
        "reference.vector",
        &analysis,
        &shift,
        &scale,
        &final_rows,
    )
    .await;
    let expected = observe(std::slice::from_ref(&reference), &analysis, &queries).await;
    let got = observe(std::slice::from_ref(&addr), &analysis, &queries).await;
    assert_eq!(
        got, expected,
        "partitioned shard differs from the reference"
    );

    // Reopen from disk: the key and the ranges survive, the reads equal.
    handle.abort();
    let _ = (&mut handle).await;
    let (reopened, reopened_handle) =
        start_opened_node(config(Some(index_path.clone()), &analysis)).await;
    let manifest = read_manifest(&index_path);
    check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    let got = observe(std::slice::from_ref(&reopened), &analysis, &queries).await;
    assert_eq!(got, expected, "reopened partitioned shard differs");

    // A later seal appends an unordered tail segment and leaves the key.
    let extra = row(N + 7, false);
    let mut node = client(&reopened).await;
    append(&mut node, &extra).await;
    node.flush(FlushRequest {}).await.unwrap();
    let manifest = read_manifest(&index_path);
    assert_eq!(manifest.partition_key.as_deref(), Some("num"));
    let tail: Vec<&SegmentMetadata> = manifest
        .segments
        .iter()
        .filter(|s| !s.segment_id.starts_with("cmp-000001-"))
        .collect();
    assert_eq!(tail.len(), 1, "one sealed tail segment");
    assert_eq!(tail[0].rows, 1);
    assert!(tail[0].summary.as_ref().unwrap().partition.is_none());
    assert_eq!(int_summary(tail[0], "num"), (extra.num, extra.num, 1));

    // The next partitioned compaction folds the tail in.
    let second = compact(&reopened, partition_request("num", bound))
        .await
        .unwrap();
    assert_eq!(second.partition_column, "num");
    assert_eq!(second.rows_after, live_rows + 1);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000002-", u64::from(bound));
    assert!(unkeyed.is_empty());
    assert_eq!(
        keyed.len(),
        manifest.segments.len(),
        "no other segment remains"
    );
    assert_eq!(keyed.iter().map(|s| s.rows).sum::<u64>(), live_rows + 1);

    // And the bucket layout is back on request: no key, no ranges.
    let third = compact(&reopened, CompactShardRequest::default())
        .await
        .unwrap();
    assert_eq!(third.partition_column, "");
    let manifest = read_manifest(&index_path);
    assert_eq!(manifest.partition_key, None);
    assert!(manifest
        .segments
        .iter()
        .all(|s| s.summary.as_ref().unwrap().partition.is_none()));
    let got = observe(std::slice::from_ref(&reopened), &analysis, &queries).await;
    let mut node = client(&reference).await;
    append(&mut node, &extra).await;
    node.flush(FlushRequest {}).await.unwrap();
    let expected = observe(std::slice::from_ref(&reference), &analysis, &queries).await;
    assert_eq!(
        got, expected,
        "the bucket layout differs from the reference"
    );

    reopened_handle.abort();
    reference_handle.abort();
}

/// A document with no `num`: it has a vector and a facet, and no
/// integer value at all.
async fn append_without_num(client: &mut NodeServiceClient<Channel>, i: usize) -> Row {
    let mut bare = row(i, false);
    bare.key = format!("bare/{i}");
    bare.text = format!("bare{i} common {}", bare.grp);
    let doc = AddDocumentsRequest {
        text: bare.text.clone(),
        facets: vec![FacetValue {
            field: "grp".into(),
            value: bare.grp.to_string(),
        }],
        ..Default::default()
    };
    let mut request = Request::new(tokio_stream::iter([doc]));
    request.metadata_mut().insert_bin(
        pipestream_search::replication::STABLE_KEYS_METADATA,
        stable_metadata(bare.key.as_bytes()),
    );
    client.add_documents(request).await.unwrap();
    add_vector(client, &bare).await;
    bare
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_without_the_partition_column_seal_apart() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition-unkeyed");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let index_path = dir.join("shard.vector");
    let (addr, _handle) = start_empty_node(config(Some(index_path.clone()), &analysis)).await;
    calibrate(&addr, &shift, &scale).await;
    let mut node = client(&addr).await;
    // Keyed and unkeyed rows interleaved, so the unkeyed ones are spread
    // across the WAL buckets.
    let keyed_rows = 150usize;
    let unkeyed_rows = 20usize;
    for i in 0..keyed_rows + unkeyed_rows {
        if i % 8 == 3 && i / 8 < unkeyed_rows {
            append_without_num(&mut node, 10_000 + i).await;
        } else {
            append(&mut node, &row(i, false)).await;
        }
    }
    node.flush(FlushRequest {}).await.unwrap();
    let total = (keyed_rows + unkeyed_rows) as u64;
    let unkeyed_count = (0..keyed_rows + unkeyed_rows)
        .filter(|i| i % 8 == 3 && i / 8 < unkeyed_rows)
        .count() as u64;
    let queries: Vec<Vec<f32>> = (0..2)
        .map(|q| unit_vectors(1, DIM, 0x7A11_0000 + q))
        .collect();
    let before = observe(std::slice::from_ref(&addr), &analysis, &queries).await;

    let bound = 40u32;
    let response = compact(&addr, partition_request("num", bound))
        .await
        .unwrap();
    assert_eq!(response.rows_after, total);
    let manifest = read_manifest(&index_path);
    let (keyed, unkeyed) = check_partitions(&manifest, "cmp-000001-", u64::from(bound));
    assert_eq!(unkeyed.len(), 1, "one unkeyed segment under the bound");
    assert_eq!(unkeyed[0].rows, unkeyed_count);
    assert_eq!(
        keyed.iter().map(|s| s.rows).sum::<u64>(),
        total - unkeyed_count
    );
    // The unkeyed segment holds exactly the bare rows.
    let ids: Vec<u64> = (unkeyed[0].base_label..unkeyed[0].base_label + unkeyed[0].rows).collect();
    let fetched = node
        .get_documents(GetDocumentsRequest { doc_ids: ids })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.documents.len() as u64, unkeyed_count);
    assert!(fetched.documents.iter().all(|d| d.text.starts_with("bare")));
    let after = observe(std::slice::from_ref(&addr), &analysis, &queries).await;
    assert_eq!(after, before, "the partitioned shard reads differently");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_compaction_rejects_the_wrong_column() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = tempdir("partition-refusals");
    let sample = unit_vectors(64, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);

    let (segmented, _segmented_handle) =
        start_empty_node(config(Some(dir.join("segmented.vector")), &analysis)).await;
    calibrate(&segmented, &shift, &scale).await;
    let mut node = client(&segmented).await;
    for i in 0..40 {
        append(&mut node, &row(i, false)).await;
    }
    node.flush(FlushRequest {}).await.unwrap();
    expect_refusal(
        compact(&segmented, partition_request("grp", 16)).await,
        tonic::Code::InvalidArgument,
        "a facet column",
    )
    .await;
    expect_refusal(
        compact(&segmented, partition_request("score", 16)).await,
        tonic::Code::InvalidArgument,
        "a double column",
    )
    .await;
    expect_refusal(
        compact(&segmented, partition_request("nope", 16)).await,
        tonic::Code::InvalidArgument,
        "not a column of this shard",
    )
    .await;
    // An integer column of the table no document carries.
    expect_refusal(
        compact(&segmented, partition_request("other", 16)).await,
        tonic::Code::FailedPrecondition,
        "no document of this shard carries it",
    )
    .await;
    // The refusals wrote nothing: the seeded segment is the only one.
    let manifest = read_manifest(&dir.join("segmented.vector"));
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.partition_key, None);
    // A dry run echoes the column and writes nothing.
    let dry = compact(
        &segmented,
        CompactShardRequest {
            dry_run: true,
            ..partition_request("num", 16)
        },
    )
    .await
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.partition_column, "num");
    assert_eq!(
        read_manifest(&dir.join("segmented.vector")).segments.len(),
        1
    );
}
