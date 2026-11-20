//! Hybrid round validation and shared ranking regressions.

use super::hybrid_context::{
    local_fusion_request, CascadeQuery, HybridInput, HybridQuery, HybridRound,
};
use super::hybrid_reduce::{finish_two_level, fuse_clustered_shard, GlobalHybrid};
use super::*;

mod validation;

fn query(filters: &RequestFilters) -> HybridQuery<'_> {
    let input = HybridInput::new("request-17", "search", &[1.0], 2, None, filters).unwrap();
    HybridQuery::new(input, default_legs(), false).unwrap()
}

fn default_legs() -> HybridLegs {
    HybridLegs {
        leg_k: 4,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: FusionMode::GlobalRank,
        normalization: fusion::Normalization::MinMax,
        combination: fusion::Combination::Arithmetic,
        min_vector_score: 0.0,
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
    let query = query(&filters);
    let query = HybridQuery::new(
        query.input(),
        HybridLegs {
            bm25_weight: 0.0,
            ..query.legs()
        },
        false,
    )
    .unwrap();
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
        query.legs(),
    );
    assert_eq!(local.request_id, "request-17");
    assert_eq!(local.vector_weight, 1.0);
    assert_eq!(local.bm25_weight, 0.0);
    assert_eq!(local.expected_stats_incarnation, [7; 32]);
}

#[test]
fn global_vector_floor_filters_both_legs_before_truncation() {
    let filters = RequestFilters::default();
    let input = HybridInput::new("request-17", "search", &[1.0], 1, None, &filters).unwrap();
    let query = HybridQuery::new(
        input,
        HybridLegs {
            min_vector_score: 0.5,
            ..default_legs()
        },
        false,
    )
    .unwrap();
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
    let query = query(&filters);
    let query = HybridQuery::new(
        query.input(),
        HybridLegs {
            fusion_mode: FusionMode::TwoLevel,
            ..query.legs()
        },
        false,
    )
    .unwrap();
    let first = fuse_clustered_shard(
        vec![(10, 0.8)],
        vec![raw(10, 4.0, "first source")],
        query.legs(),
    );
    let second = fuse_clustered_shard(
        vec![(20, 0.7)],
        vec![raw(20, 3.0, "second source")],
        query.legs(),
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

#[test]
fn cascade_tie_pool_survives_split_shards_and_reranking_keeps_candidates() {
    let filters = RequestFilters::default();
    let input = HybridInput::new("id", "", &[1.0], 2, None, &filters).unwrap();
    let query = CascadeQuery::new(input, 0.0, false).unwrap();
    let result = FanoutResult {
        shard_hits: vec![(0, vec![(1, 0.9), (4, 0.8)]), (1, vec![(2, 0.8), (3, 0.7)])],
        hits: Vec::new(),
        shard_stats: Vec::new(),
        shard_wall_ms: Vec::new(),
        hedges_fired: 0,
        hedge_wins: 0,
        rerank_candidates: HashMap::new(),
    };
    let pool = super::cascade::cascade_pool(&result, query);
    assert_eq!(pool.iter().map(|hit| hit.0).collect::<Vec<_>>(), [1, 2, 4]);
    let mut candidates = HashMap::from([(
        4,
        crate::pb::RerankCandidatePayload {
            text: "winner".into(),
            ..Default::default()
        },
    )]);
    let hits = super::cascade::rank_cascade(
        pool,
        &HashMap::from([(1, 1.0), (2, 5.0), (4, 6.0)]),
        &mut candidates,
        2,
    );
    assert_eq!(
        hits.iter()
            .map(|hit| (hit.doc_id, hit.rank, hit.shard))
            .collect::<Vec<_>>(),
        [(4, 1, 0), (2, 2, 1)]
    );
    assert_eq!(hits[0].rerank_candidate.as_ref().unwrap().text, "winner");
}
