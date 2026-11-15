//! The lexical leaf's tree, built from the shard's scoring breakdown.

use tonic::Status;

use super::{leaf, node};
use crate::pb::{
    Bm25Hit, Bm25TermExplain, Explanation, PrefixExpansion, ScoreStageExplain, SynonymExpansion,
};

/// The lexical leaf's tree from the shard's breakdown: one node per
/// (field, term) contribution with the BM25 inputs under it, prefix
/// and synonym expansions grouped under the term that produced them,
/// the sum, then the score stages in evaluation order, then the leaf's
/// served f32.
pub fn lexical(
    id: &str,
    hit: &Bm25Hit,
    prefixes: &[PrefixExpansion],
    synonyms: &[SynonymExpansion],
) -> Result<Explanation, Status> {
    let breakdown = hit.explain.as_ref().ok_or_else(|| {
        Status::internal(format!(
            "explain: the lexical route returned document {} without its scoring breakdown",
            hit.doc_id
        ))
    })?;
    // Which source produced each scored term: a prefix, a synonym
    // rule, or the query text itself. A term can be both a query term
    // and an expansion of another; the query text wins, so a term the
    // user typed is never filed under a rule.
    let mut term_nodes: Vec<Explanation> = Vec::new();
    let mut groups: Vec<(String, Vec<Explanation>)> = Vec::new();
    for term in &breakdown.terms {
        let field = field_label(term);
        let term_node = term_node(term, &field);
        match expansion_source(term, &field, prefixes, synonyms) {
            Some(label) => match groups.iter_mut().find(|(l, _)| *l == label) {
                Some((_, list)) => list.push(term_node),
                None => groups.push((label, vec![term_node])),
            },
            None => term_nodes.push(term_node),
        }
    }
    for (label, list) in groups {
        let sum: f64 = list.iter().map(|n| n.value).sum();
        term_nodes.push(node(
            sum,
            format!("{label}: sum of the expansions' contributions"),
            list,
        ));
    }
    let sum_description = if breakdown.phrase {
        format!(
            "BM25 sum over {} (field, term) contributions: the base fields summed plus the \
             phrase group's maximum ({})",
            breakdown.terms.len(),
            breakdown.phrase_max
        )
    } else {
        format!(
            "BM25 sum over {} (field, term) contributions",
            breakdown.terms.len()
        )
    };
    let mut current = node(breakdown.bm25, sum_description, term_nodes);
    for stage in &breakdown.stages {
        current = stage_node(stage, current);
    }
    Ok(node(
        f64::from(hit.score),
        format!("lexical leaf {id:?}: BM25 relevance, served as f32 of the arithmetic below"),
        vec![current],
    ))
}

fn field_label(term: &Bm25TermExplain) -> String {
    if term.field.is_empty() {
        "body".to_string()
    } else {
        term.field.clone()
    }
}

/// One (field, term) contribution with its BM25 inputs under it.
fn term_node(term: &Bm25TermExplain, field: &str) -> Explanation {
    let mut inputs = vec![
        leaf(
            term.tf_norm,
            format!(
                "tf_norm = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl)) with tf={}, \
                 dl={}, avgdl={}, k1={}, b={}",
                term.tf, term.doc_length, term.avgdl, term.k1, term.b
            ),
        ),
        leaf(
            term.idf,
            format!(
                "idf = ln(1 + (N - df + 0.5) / (df + 0.5)) with N={}, df={}",
                term.doc_count, term.df
            ),
        ),
        leaf(term.weight, "field weight"),
    ];
    let description = if term.phrase_group {
        inputs.push(leaf(term.phrase_weight, "phrase term weight"));
        format!(
            "term {:?} in field {field}: weight * idf * tf_norm * phrase weight (phrase \
             group, combined by maximum)",
            term.term
        )
    } else {
        format!(
            "term {:?} in field {field}: weight * idf * tf_norm",
            term.term
        )
    };
    node(term.contribution, description, inputs)
}

/// The label of the prefix or synonym rule that produced `term`, when
/// the query text did not.
fn expansion_source(
    term: &Bm25TermExplain,
    field: &str,
    prefixes: &[PrefixExpansion],
    synonyms: &[SynonymExpansion],
) -> Option<String> {
    prefixes
        .iter()
        .find(|p| p.field == term.field && p.terms.contains(&term.term))
        .map(|p| format!("expansions of prefix {:?} in field {field}", p.prefix))
        .or_else(|| {
            synonyms
                .iter()
                .find(|s| s.field == term.field && s.terms.contains(&term.term))
                .map(|s| format!("synonyms of {:?} in field {field}", s.term))
        })
}

/// One score stage applied to the node below it.
fn stage_node(stage: &ScoreStageExplain, current: Explanation) -> Explanation {
    let key = stage
        .map_key
        .as_deref()
        .or_else(|| (!stage.key.is_empty()).then_some(stage.key.as_str()));
    let name = match key {
        None => stage.column.clone(),
        Some(key) => format!("{}[{key:?}]", stage.column),
    };
    let description = if stage.present {
        format!(
            "score stage {} on column {name}: input {} gives contribution {} applied to \
             the node below (docs/score-functions.md)",
            stage.stage, stage.input, stage.contribution
        )
    } else {
        format!(
            "score stage {} on column {name}: the document has no value, identity",
            stage.stage
        )
    };
    node(stage.output, description, vec![current])
}
