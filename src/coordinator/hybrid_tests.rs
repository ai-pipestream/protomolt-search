//! Hybrid round validation and shared ranking regressions.

use super::hybrid_context::{local_fusion_request, HybridQuery, HybridRound};
use super::hybrid_reduce::{finish_two_level, fuse_clustered_shard, GlobalHybrid};
use super::*;

fn query(filters: &RequestFilters) -> HybridQuery<'_> {
    HybridQuery {
        request_id: "request-17",
        text: "search",
        pre_analyzed_terms: &[],
        vector: &[1.0],
        k: 2,
        spec: None,
        legs: HybridLegs {
            leg_k: 4,
            vector_weight: 1.0,
            bm25_weight: 1.0,
            rrf_k: 60.0,
            fusion_mode: FusionMode::GlobalRank,
            normalization: fusion::Normalization::MinMax,
            combination: fusion::Combination::Arithmetic,
            min_vector_score: 0.0,
        },
        debug: false,
        filters,
    }
}

fn raw(id: u64, score: f32, text: &str) -> crate::pb::RawLegHit {
    crate::pb::RawLegHit {
        doc_id: id,
        score,
        rerank_candidate: Some(crate::pb::RerankCandidatePayload {
            text: text.into(),
            ..Default::default()
        }),
    }
}

#[test]
fn hybrid_round_rejects_misaligned_statistics_before_fanout() {
    let filters = RequestFilters::default();
    let terms = vec!["search".into()];
    let global = CorpusStats {
        doc_count: 2,
        total_doc_length: 8,
        dfs: vec![1],
    };
    let claims = [StatsClaim::default()];
    assert_eq!(
        HybridRound::new(query(&filters), &terms, 3, &global, &[], 1)
            .err()
            .unwrap()
            .code(),
        tonic::Code::Internal
    );
    assert_eq!(
        HybridRound::new(query(&filters), &[], 3, &global, &claims, 1)
            .err()
            .unwrap()
            .code(),
        tonic::Code::Internal
    );
    assert!(HybridRound::new(query(&filters), &terms, 3, &global, &claims, 1).is_ok());
}

#[test]
fn hybrid_wire_context_preserves_route_identity_and_disabled_legs() {
    let filters = RequestFilters {
        include_rerank_candidate: true,
        ..Default::default()
    };
    let terms = vec!["search".into()];
    let global = CorpusStats {
        doc_count: 2,
        total_doc_length: 8,
        dfs: vec![1],
    };
    let claims = [StatsClaim::from_wire(9, &[7; 32]).unwrap()];
    let mut query = query(&filters);
    query.legs.bm25_weight = 0.0;
    let round = HybridRound::new(query, &terms, 43, &global, &claims, 1).unwrap();
    let plain = round.shard_request(Bm25Params::default(), 0, None, None, false, false);
    assert_eq!(plain.request_id, "");
    assert_eq!(plain.vector, [1.0]);
    assert!(plain.terms.is_empty());
    assert!(plain.global_doc_frequencies.is_empty());
    assert_eq!(plain.analysis_fingerprint, 43);
    assert_eq!(plain.expected_stats_epoch, 9);
    assert_eq!(plain.expected_stats_incarnation, [7; 32]);
    assert!(plain.include_rerank_candidate);
    let clustered = round.shard_request(Bm25Params::default(), 0, None, None, true, false);
    assert_eq!(clustered.request_id, "request-17");
    assert!(clustered.vector.is_empty());
    let local = local_fusion_request(
        round.shard_request(Bm25Params::default(), 0, None, None, false, true),
        query.legs,
    );
    assert_eq!(local.request_id, "request-17");
    assert_eq!(local.vector_weight, 1.0);
    assert_eq!(local.bm25_weight, 0.0);
    assert_eq!(local.expected_stats_incarnation, [7; 32]);
}

#[test]
fn global_vector_floor_filters_both_legs_before_truncation() {
    let filters = RequestFilters::default();
    let mut query = query(&filters);
    query.k = 1;
    query.legs.min_vector_score = 0.5;
    let mut results = GlobalHybrid::default();
    results.collect_raw(
        2,
        vec![raw(20, 0.9, "vector winner"), raw(10, 0.1, "too low")],
        true,
    );
    results.collect_raw(
        2,
        vec![
            raw(10, 100.0, "lexical leader"),
            raw(30, 99.0, "lexical only"),
            raw(20, 1.0, "lexical winner"),
        ],
        false,
    );
    let hits = results.finish(query);
    assert_eq!(hits.len(), 1);
    assert_eq!((hits[0].doc_id, hits[0].shard), (20, 2));
    assert_eq!(hits[0].vector_rank, Some(1));
    assert_eq!(hits[0].bm25_rank, Some(1));
    assert_eq!(
        hits[0].rerank_candidate.as_ref().unwrap().text,
        "vector winner"
    );
}

#[test]
fn global_fusion_keeps_lower_id_ties_and_first_source_payload() {
    let filters = RequestFilters::default();
    let mut results = GlobalHybrid::default();
    results.collect_raw(4, vec![raw(10, 2.0, "first vector")], true);
    results.collect_raw(4, vec![raw(20, 2.0, "first lexical")], false);
    results.collect_raw(7, vec![raw(20, 1.0, "later vector")], true);
    results.collect_raw(7, vec![raw(10, 1.0, "later lexical")], false);
    let hits = results.finish(query(&filters));
    assert_eq!(
        hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
        [10, 20]
    );
    assert_eq!(hits.iter().map(|hit| hit.shard).collect::<Vec<_>>(), [4, 4]);
    assert_eq!(
        hits[0].rerank_candidate.as_ref().unwrap().text,
        "first vector"
    );
    assert_eq!(
        hits[1].rerank_candidate.as_ref().unwrap().text,
        "first lexical"
    );
}

#[test]
fn two_level_fusion_retains_local_provenance_and_candidate_payload() {
    let filters = RequestFilters::default();
    let mut query = query(&filters);
    query.legs.fusion_mode = FusionMode::TwoLevel;
    let first = fuse_clustered_shard(
        vec![(10, 0.8)],
        vec![raw(10, 4.0, "first source")],
        query.legs,
    );
    let second = fuse_clustered_shard(
        vec![(20, 0.7)],
        vec![raw(20, 3.0, "second source")],
        query.legs,
    );
    let hits = finish_two_level(vec![(3, first), (9, second)], query);
    assert_eq!(
        hits.iter()
            .map(|hit| (hit.doc_id, hit.shard))
            .collect::<Vec<_>>(),
        [(10, 3), (20, 9)]
    );
    assert_eq!(hits[0].vector_score, 0.8);
    assert_eq!(hits[0].bm25_score, 4.0);
    assert_eq!(hits[0].vector_rank, Some(1));
    assert_eq!(hits[0].bm25_rank, Some(1));
    assert_eq!(
        hits[0].rerank_candidate.as_ref().unwrap().text,
        "first source"
    );
    assert_eq!(
        hits[1].rerank_candidate.as_ref().unwrap().text,
        "second source"
    );
}

#[test]
fn hybrid_receipts_cannot_be_silently_accepted_without_admission() {
    let receipt = crate::pb::VectorReadReceipt::default();
    assert!(super::hybrid::accept_hybrid_receipt(&None, 0, None, "mismatch").is_ok());
    let error =
        super::hybrid::accept_hybrid_receipt(&None, 0, Some(&receipt), "mismatch").unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "mismatch");
    let claim = StatsClaim::from_wire(9, &[7; 32]).unwrap();
    let admission =
        crate::vector_read::VectorReadBarrier::new("body".into(), None, vec![claim]).unwrap();
    assert_eq!(
        super::hybrid::accept_hybrid_receipt(&Some(admission), 0, None, "missing")
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}
