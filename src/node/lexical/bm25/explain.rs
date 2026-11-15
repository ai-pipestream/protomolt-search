//! Per-hit explain trees: term contributions and score-chain replay.

use crate::node::*;

/// Explain term contributions in scoring order, with optional phrase weights.
pub(in crate::node) fn explain_terms(
    fields: &[bm25::FieldQuery<'_>],
    names: &[&str],
    presence: &[bm25::TermPresence],
    phrase: Option<(usize, &[f64])>,
) -> crate::pb::Bm25Explain {
    let mut base = 0.0f64;
    let mut phrase_max = 0.0f64;
    let mut terms = Vec::with_capacity(presence.len());
    for hit in presence {
        let fq = &fields[hit.field];
        let avgdl = fq.stats.avgdl();
        let idf = bm25::idf(fq.stats.doc_count, fq.stats.dfs[hit.term]);
        let tf_norm = bm25::tf_norm(fq.params, hit.tf, hit.doc_len, avgdl);
        let mut contribution = fq.weight * idf * tf_norm;
        let mut phrase_group = false;
        let mut phrase_weight = 1.0;
        match phrase {
            Some((view, weights)) if view == hit.field => {
                phrase_group = true;
                phrase_weight = weights[hit.term];
                contribution *= phrase_weight;
                phrase_max = phrase_max.max(contribution);
            }
            _ => base += contribution,
        }
        terms.push(crate::pb::Bm25TermExplain {
            field: names[hit.field].to_string(),
            term: fq.terms[hit.term].clone(),
            tf: hit.tf,
            doc_length: hit.doc_len,
            avgdl,
            k1: fq.params.k1,
            b: fq.params.b,
            tf_norm,
            doc_count: fq.stats.doc_count,
            df: fq.stats.dfs[hit.term],
            idf,
            weight: fq.weight,
            contribution,
            phrase_group,
            phrase_weight,
        });
    }
    crate::pb::Bm25Explain {
        terms,
        bm25: base + phrase_max,
        stages: Vec::new(),
        phrase: phrase.is_some(),
        phrase_max,
    }
}

/// Replay the score chain using the scorer's column reads and arithmetic.
pub(super) fn explain_stages(
    explain: &mut crate::pb::Bm25Explain,
    chain: &crate::scorefn::ScoreChain,
    specs: &[crate::pb::ScoreStage],
    doc_id: u32,
    columns: &dyn crate::scorefn::NumericRead,
) {
    let mut score = explain.bm25;
    for (i, stage) in chain.stages.iter().enumerate() {
        let spec = &specs[i];
        let mut row = crate::pb::ScoreStageExplain {
            stage: i as u32,
            column: spec.column.clone(),
            key: spec.map_key().unwrap_or_default().to_owned(),
            map_key: matches!(
                spec.operation,
                Some(
                    crate::pb::score_stage::Operation::MapOp(_)
                        | crate::pb::score_stage::Operation::TypedMapOp(_)
                )
            )
            .then(|| spec.map_key().expect("explicit map selector").to_owned()),
            present: false,
            input: 0.0,
            contribution: 0.0,
            output: score,
        };
        if let (Some(input), Some(contribution)) = (
            stage.input(doc_id, columns),
            stage.contribution(doc_id, columns),
        ) {
            score = if stage.is_additive() {
                score + contribution
            } else {
                score * contribution
            };
            row.present = true;
            row.input = input;
            row.contribution = contribution;
            row.output = score;
        }
        explain.stages.push(row);
    }
}
