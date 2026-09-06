//! Physical execution metadata has a separate disclosure boundary from fields.
//! Selection must already have enforced the document view before this runs.
use crate::pb::*;

pub(crate) fn redact_execution(response: &mut QueryResponse) {
    // Exhaustive patterns make new response fields require a disclosure choice.
    let QueryResponse {
        request_id: _,
        hits: _,
        executed: _,
        next_cursor: _,
        profile,
        dense_quality,
        dense_execution,
        served_topology_generation: _,
        aggregate: _,
        groups: _,
        synonym_expansions: _,
        field_details_redacted: _,
        execution_details_redacted,
    } = response;
    *execution_details_redacted = true;
    if let Some(QueryProfile {
        selection_ms: _,
        boost_ms: _,
        values_ms: _,
        scorer_ms: _,
        projection_ms: _,
        total_ms: _,
        rerank_ms: _,
        collapse_ms: _,
        rerank_rows,
        rerank_logical_bytes,
        rerank_pages,
        rerank_tasks,
        segments_total,
        segments_skipped,
        shards_total,
        shards_skipped,
    }) = profile
    {
        *rerank_rows = 0;
        *rerank_logical_bytes = 0;
        *rerank_pages = 0;
        *rerank_tasks = 0;
        *segments_total = 0;
        *segments_skipped = 0;
        *shards_total = 0;
        *shards_skipped = 0;
    }
    if let Some(DenseQualityOutcome {
        target_recall_ppm: _,
        selection_k: _,
        profile_fingerprint: _,
        profile_id,
        embedding_model,
        corpus_generation,
        corpus_rows,
        provider_backend: _,
        scoring_fingerprint: _,
        evidence_scope: _,
    }) = dense_quality
    {
        profile_id.clear();
        embedding_model.clear();
        *corpus_generation = 0;
        *corpus_rows = 0;
    }
    if let Some(DenseExecutionOutcome {
        requested_mode: _,
        resolved_mode: _,
        provider_backend: _,
        quality_contract: _,
        scoring_fingerprint: _,
        exhaustive_completion: _,
        planner_reason,
        policy_id,
        policy_fingerprint: _,
        policy_point,
        filter_selectivity_ppm,
        candidate_depth: _,
        evidence_scope: _,
    }) = dense_execution
    {
        planner_reason.clear();
        policy_id.clear();
        *filter_selectivity_ppm = 0;
        if let Some(DensePolicyPoint {
            k: _,
            filter_selectivity_ppm_min,
            filter_selectivity_ppm_max,
            candidates: _,
            measured_recall_ppm: _,
        }) = policy_point
        {
            *filter_selectivity_ppm_min = 0;
            *filter_selectivity_ppm_max = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn physical_details_are_withheld_without_erasing_result_or_evidence_semantics() {
        let mut response = QueryResponse {
            request_id: "request".into(),
            hits: vec![QueryHit {
                doc_id: 7,
                score: 0.25,
                rank: 1,
                ..Default::default()
            }],
            executed: "search+fp32_rerank".into(),
            next_cursor: "opaque-cursor".into(),
            served_topology_generation: 9,
            field_details_redacted: true,
            profile: Some(QueryProfile {
                selection_ms: 1.0,
                boost_ms: 2.0,
                values_ms: 3.0,
                scorer_ms: 4.0,
                projection_ms: 5.0,
                total_ms: 6.0,
                rerank_ms: 7.0,
                collapse_ms: 8.0,
                rerank_rows: 11,
                rerank_logical_bytes: 12,
                rerank_pages: 13,
                rerank_tasks: 14,
                segments_total: 15,
                segments_skipped: 16,
                shards_total: 17,
                shards_skipped: 18,
            }),
            dense_quality: Some(DenseQualityOutcome {
                target_recall_ppm: 990_000,
                selection_k: 100,
                profile_fingerprint: "a".repeat(64),
                profile_id: "SECRET-profile".into(),
                embedding_model: "SECRET-model".into(),
                corpus_generation: 33,
                corpus_rows: 999_999,
                provider_backend: "backend".into(),
                scoring_fingerprint: "score-space".into(),
                evidence_scope: DenseEvidenceScope::CorpusBenchmark as i32,
            }),
            dense_execution: Some(DenseExecutionOutcome {
                requested_mode: DenseExecutionMode::Auto as i32,
                resolved_mode: DenseExecutionMode::Ann as i32,
                provider_backend: "backend".into(),
                quality_contract: VectorQualityContract::ConfiguredAnn as i32,
                scoring_fingerprint: "score-space".into(),
                exhaustive_completion: false,
                planner_reason: "SECRET physical corpus and planner details".into(),
                policy_id: "SECRET-policy".into(),
                policy_fingerprint: "b".repeat(64),
                policy_point: Some(DensePolicyPoint {
                    k: 10,
                    filter_selectivity_ppm_min: 100_000,
                    filter_selectivity_ppm_max: 600_000,
                    candidates: 100,
                    measured_recall_ppm: 980_000,
                }),
                filter_selectivity_ppm: 500_000,
                candidate_depth: 100,
                evidence_scope: DenseEvidenceScope::SelectivityBandBenchmark as i32,
            }),
            ..Default::default()
        };
        let before = response.clone();
        redact_execution(&mut response);
        let bytes = response.encode_to_vec();
        assert!(!bytes.windows(6).any(|window| window == b"SECRET"));
        let decoded = QueryResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded, response);
        assert!(response.execution_details_redacted && response.field_details_redacted);
        assert_eq!(response.hits, before.hits);
        assert_eq!(response.executed, before.executed);
        assert_eq!(response.next_cursor, before.next_cursor);
        assert_eq!(
            response.served_topology_generation,
            before.served_topology_generation
        );
        let profile = response.profile.as_ref().unwrap();
        assert_eq!(
            profile,
            &QueryProfile {
                selection_ms: 1.0,
                boost_ms: 2.0,
                values_ms: 3.0,
                scorer_ms: 4.0,
                projection_ms: 5.0,
                total_ms: 6.0,
                rerank_ms: 7.0,
                collapse_ms: 8.0,
                ..Default::default()
            }
        );
        let quality = response.dense_quality.as_ref().unwrap();
        assert_eq!((quality.corpus_rows, quality.corpus_generation), (0, 0));
        assert_eq!(
            (quality.target_recall_ppm, quality.selection_k),
            (990_000, 100)
        );
        assert_eq!(
            quality.evidence_scope,
            DenseEvidenceScope::CorpusBenchmark as i32
        );
        assert_eq!(quality.profile_fingerprint, "a".repeat(64));
        let execution = response.dense_execution.as_ref().unwrap();
        assert_eq!(execution.filter_selectivity_ppm, 0);
        assert!(!execution.exhaustive_completion);
        assert_eq!(execution.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(execution.candidate_depth, 100);
        assert_eq!(execution.policy_fingerprint, "b".repeat(64));
        assert_eq!(
            execution.policy_point,
            Some(DensePolicyPoint {
                k: 10,
                candidates: 100,
                measured_recall_ppm: 980_000,
                ..Default::default()
            })
        );
        let once = response.clone();
        redact_execution(&mut response);
        assert_eq!(response, once);
    }

    #[test]
    fn an_empty_response_still_declares_its_disclosure_boundary() {
        let mut response = QueryResponse::default();
        redact_execution(&mut response);
        assert!(response.execution_details_redacted);
        assert!(response.profile.is_none());
        assert!(response.dense_quality.is_none());
        assert!(response.dense_execution.is_none());
    }
}
