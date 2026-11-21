//! Coalesced vector scan authority and the parent-map cache.

use super::*;

/// A top-10 scan of the constant vector under `context`, with no filters,
/// floor exchange, or patience options.
fn scan_job(
    context: crate::pb::VectorReadContext,
    ready_tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    done: tokio::sync::oneshot::Sender<Result<ScanOutcome, Status>>,
) -> ScanJob {
    ScanJob {
        read_context: Some(context),
        ready_tx,
        vector: vec![0.25; 16],
        k: 10,
        tie_complete: false,
        geo_filters: vec![],
        geo_regions: vec![],
        filter: None,
        enumerate_max_rows: None,
        patience_saturation_ppm: None,
        patience_hops: None,
        floor_prune: None,
        floor_k: None,
        floor_share_ppm: None,
        floor_greediness_ppm: None,
        floor_clamp_min: None,
        include_rerank_candidate: false,
        external: Box::new(|| None),
        publish: Box::new(|_| false),
        done,
    }
}

#[test]
fn one_coalesced_pass_keeps_each_jobs_authority_and_rejects_only_the_bad_job() {
    let mut store = crate::postings::Bm25Store::new()
        .with_columns(crate::postings::ColumnKind::Facet, &["audience"]);
    for (row, audience) in ["public", "private"].into_iter().enumerate() {
        store.add_document(
            row as u32,
            "word".into(),
            crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
        );
        store.set_facet(0, row as u32, audience);
    }
    let mut index =
        crate::vector::VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, 16, 4).unwrap();
    index.add(&[0.25; 48], 16).unwrap(); // final row has no document metadata
    let node = NodeServiceImpl::new(Some(index), Default::default())
        .with_bm25(Some(Bm25Shard::Building(store)));
    let mut jobs = Vec::new();
    let mut outputs = Vec::new();
    for (field, predicate, expected) in [
        ("unbound", "audience == 'public'", None),
        ("", "audience == 'public'", Some(vec![0])),
        ("", "audience == 'private'", Some(vec![1])),
        ("", "!has(audience)", Some(vec![])),
    ] {
        let context = crate::pb::VectorReadContext {
            field: field.into(),
            visibility: Some(crate::pb::DocumentVisibility {
                filter: crate::cel::compile_filter(predicate).unwrap(),
            }),
            ..Default::default()
        };
        let scope = crate::visibility::VisibilityScope::new(context.visibility.as_ref()).unwrap();
        let (ready_tx, ready_rx) = mpsc::channel(4);
        let (done, outcome) = tokio::sync::oneshot::channel();
        jobs.push(scan_job(context, ready_tx, done));
        outputs.push((scope, expected, ready_rx, outcome));
    }
    run_scan_batch(&node.state, 1, true, jobs);
    for (scope, expected, mut ready, outcome) in outputs {
        let result = outcome.blocking_recv().unwrap();
        if let Some(expected) = expected {
            let search_shard_response::Payload::ReadReady(receipt) =
                ready.blocking_recv().unwrap().unwrap().payload.unwrap()
            else {
                panic!("missing read receipt")
            };
            scope
                .validate_echo(
                    &receipt.visibility_fingerprint,
                    &receipt.visibility_columns_known,
                )
                .unwrap();
            assert_eq!(receipt.visibility_columns_known, vec![true]);
            assert_eq!(
                result
                    .unwrap()
                    .hits
                    .into_iter()
                    .map(|hit| hit.slot)
                    .collect::<Vec<_>>(),
                expected
            );
        } else {
            assert_eq!(
                result.err().unwrap().code(),
                tonic::Code::FailedPrecondition
            );
            assert!(ready.blocking_recv().is_none());
        }
    }
}
#[test]
fn parent_cache_cannot_cross_same_sized_replacements_or_publish_an_old_build() {
    fn store(parent: u64) -> Bm25Shard {
        let mut store = crate::postings::Bm25Store::new();
        store.add_document_with_lineage(
            0,
            "word".into(),
            crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
            Some(crate::postings::DocLineage {
                parent_id: parent,
                group_id: 0,
                span_start: 0,
                span_end: 4,
            }),
        );
        Bm25Shard::Building(store)
    }
    let node = NodeServiceImpl::new(None, Default::default()).with_bm25(Some(store(10)));
    let old = NodeServiceImpl::parent_map(&node.state, 100, 1).unwrap();
    assert_eq!(old.rows.as_slice(), &[10]);
    let node = node.with_bm25(Some(store(20)));
    let fresh = NodeServiceImpl::parent_map(&node.state, 100, 1).unwrap();
    assert_eq!(fresh.rows.as_slice(), &[20]);
    assert_ne!(fresh.claim, old.claim);
    let mut guard = write_shard(&node.state);
    // This is the publication gap: an old read finishes building after
    // a replacement and must not overwrite its cache with old lineage.
    assert_eq!(
        guard.publish_parent_map(&old).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(guard.parents.as_ref().unwrap().rows.as_slice(), &[20]);
    // The scan takes its final read guard after obtaining the cache.
    assert!(guard
        .check_stats_epoch(old.claim.epoch, &old.claim.incarnation())
        .is_err());
    guard
        .check_stats_epoch(fresh.claim.epoch, &fresh.claim.incarnation())
        .unwrap();
}
