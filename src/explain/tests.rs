use super::*;
use crate::pb::{
    Bm25Hit, FusionMode, HybridHit, PrefixExpansion, ScoreCombination, ScoreNormalization,
};

#[test]
fn a_lexical_tree_recomposes_its_sum_and_groups_expansions() {
    let hit = Bm25Hit {
        identity: None,
        doc_id: 7,
        score: 1.5,
        terms: Vec::new(),
        projected: Vec::new(),
        snippets: Vec::new(),
        explain: Some(crate::pb::Bm25Explain {
            terms: vec![
                crate::pb::Bm25TermExplain {
                    term: "zebra".into(),
                    tf: 1,
                    doc_length: 3,
                    avgdl: 3.0,
                    k1: 1.2,
                    b: 0.75,
                    tf_norm: 1.0,
                    doc_count: 8,
                    df: 4,
                    idf: 1.0,
                    weight: 1.0,
                    contribution: 1.0,
                    ..Default::default()
                },
                crate::pb::Bm25TermExplain {
                    term: "zebras".into(),
                    contribution: 0.5,
                    weight: 1.0,
                    ..Default::default()
                },
            ],
            bm25: 1.5,
            stages: Vec::new(),
            phrase: false,
            phrase_max: 0.0,
        }),
    };
    let prefixes = vec![PrefixExpansion {
        field: String::new(),
        prefix: "zeb".into(),
        terms: vec!["zebras".into()],
    }];
    let tree = lexical("lex", &hit, &prefixes, &[]).unwrap();
    assert_eq!(tree.value, 1.5);
    let sum = &tree.details[0];
    assert_eq!(sum.value, 1.5);
    assert_eq!(sum.details.len(), 2, "one plain term and one prefix group");
    let group = sum
        .details
        .iter()
        .find(|n| n.description.contains("prefix \"zeb\""))
        .expect("the prefix group");
    assert_eq!(group.value, 0.5);
    assert_eq!(
        group.details[0].description,
        "term \"zebras\" in field body: weight * idf * tf_norm"
    );
}

#[test]
fn a_lexical_hit_without_a_breakdown_is_an_internal_error() {
    let hit = Bm25Hit {
        identity: None,
        doc_id: 1,
        score: 1.0,
        terms: Vec::new(),
        projected: Vec::new(),
        snippets: Vec::new(),
        explain: None,
    };
    let err = lexical("lex", &hit, &[], &[]).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
}

#[test]
fn rrf_legs_sum_to_the_fused_score() {
    let fusion = Fusion {
        mode: FusionMode::GlobalRank,
        rrf_k: 60.0,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        normalization: ScoreNormalization::MinMax,
        combination: ScoreCombination::Arithmetic,
    };
    let fused = 1.0 / 61.0 + 1.0 / 63.0;
    let hit = HybridHit {
        doc_id: 3,
        fused_score: fused as f32,
        shard: 0,
        vector_rank: Some(1),
        vector_score: 0.9,
        bm25_rank: Some(3),
        bm25_score: 2.0,
        boost_score: 0.0,
        vector_normalized: None,
        bm25_normalized: None,
        rerank_candidate: None,
    };
    let tree = composite(&hit, &fusion, "d", "l", None).unwrap();
    let sum: f64 = tree.details.iter().map(|n| n.value).sum();
    assert!((sum - tree.value).abs() < 1e-6, "{sum} vs {}", tree.value);
}
