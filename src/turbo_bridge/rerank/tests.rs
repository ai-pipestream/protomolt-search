use super::*;

#[tokio::test]
async fn calibrated_rerank_populates_snippets_on_dense_hits_and_composes_lineage_offsets() {
    let mut spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
        platt_slope: 1.5,
        platt_intercept: 0.0,
    });
    spec.compose_parent_lineage_offsets = true;

    let candidates = vec![
        RerankCandidate {
            doc_id: 10,
            prior_score: 0.88,
            text: "General corporate boilerplate without the target concept.".into(),
            sentence_spans: vec![(0, 57)],
            lineage_span_start: 100,
        },
        RerankCandidate {
            doc_id: 20,
            prior_score: 0.72,
            text: "Background preamble. Sublinear HNSW vector quantization lowers latency.".into(),
            sentence_spans: vec![(0, 20), (21, 71)],
            lineage_span_start: 500,
        },
    ];

    let hits = rerank_and_highlight_shortlist(&spec, "vector quantization latency", &candidates, 0)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc_id, 20);
    assert!((0.0..=1.0).contains(&hits[0].calibrated_probability));
    assert!(hits[0].calibrated_probability > hits[1].calibrated_probability);
    assert!(!hits[0].snippets.is_empty());
    let top_snip = &hits[0].snippets[0];
    assert_eq!(
        top_snip.text,
        "Sublinear HNSW vector quantization lowers latency."
    );
    // Composed onto parent coordinates via lineage_span_start = 500:
    assert_eq!(top_snip.start, 521);
    assert_eq!(top_snip.end, 571);
    assert_eq!(top_snip.sentence_index, Some(1));
}
