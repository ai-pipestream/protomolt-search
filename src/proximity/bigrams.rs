//! Derivation of a source field's bigram column from its positioned
//! analysis.

use super::{bigram_term, BIGRAM_SEPARATOR};
use crate::postings::{AnalyzedField, DocPositions, DocTerms};

/// Derive a source field's bigram column from its positioned analysis:
/// every pair of occurrences whose ordinals differ by exactly one, in
/// document order, becomes one occurrence of the bigram term spanning
/// from the first token's start to the second's end. The derived field's
/// length is its occurrence count (its own BM25 length normalization),
/// and it carries no positions of its own — it is a term column, not a
/// second phrase index.
///
/// Refuses (rather than guesses) when the source analysis carried no
/// positions, when the position table is malformed, or when a term
/// contains the separator.
pub fn derive_bigrams(source: &AnalyzedField) -> Result<AnalyzedField, String> {
    source.check_positions()?;
    let Some(positions) = &source.positions else {
        return Err("bigram column requires token positions, and the analysis carried none".into());
    };
    let terms = match &source.pos_lengths {
        Some(pos_lengths) => lattice_bigrams(source, positions, pos_lengths)?,
        None => linear_bigrams(source, positions)?,
    };
    let length = terms.iter().map(|(_, tf, _)| *tf).sum();
    Ok(AnalyzedField {
        sentences: None,
        terms,
        length,
        positions: None,
        ..Default::default()
    })
}

/// Bigrams over a token lattice: an arc pairs with every arc that starts
/// where it ends, one posting per distinct span.
fn lattice_bigrams(
    source: &AnalyzedField,
    positions: &DocPositions,
    pos_lengths: &[Vec<u32>],
) -> Result<DocTerms, String> {
    let mut arcs: Vec<(u32, u32, usize, (u32, u32))> = Vec::new();
    for (ti, (((term, _, offsets), ordinals), lengths)) in source
        .terms
        .iter()
        .zip(positions)
        .zip(pos_lengths)
        .enumerate()
    {
        if term.contains(BIGRAM_SEPARATOR) {
            return Err(format!(
                "term {term:?} contains the bigram separator; the analyzer contract forbids it"
            ));
        }
        for ((&span, &ordinal), &len) in offsets.iter().zip(ordinals).zip(lengths) {
            arcs.push((ordinal, len.max(1), ti, span));
        }
    }
    arcs.sort_unstable();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut terms: DocTerms = Vec::new();
    for &(first_ord, first_len, first_ti, first_span) in &arcs {
        let next_ord = first_ord.saturating_add(first_len);
        let start_idx = arcs.partition_point(|&(ord, _, _, _)| ord < next_ord);
        for &(second_ord, _, second_ti, second_span) in &arcs[start_idx..] {
            if second_ord != next_ord {
                break;
            }
            let term = bigram_term(&source.terms[first_ti].0, &source.terms[second_ti].0);
            let span = (
                first_span.0.min(second_span.0),
                first_span.1.max(second_span.1),
            );
            match index.get(&term) {
                Some(&i) => {
                    if !terms[i].2.contains(&span) {
                        terms[i].1 += 1;
                        terms[i].2.push(span);
                    }
                }
                None => {
                    index.insert(term.clone(), terms.len());
                    terms.push((term, 1, vec![span]));
                }
            }
        }
    }
    Ok(terms)
}

/// Bigrams over a linear token stream: every pair of occurrences whose
/// ordinals differ by exactly one.
fn linear_bigrams(source: &AnalyzedField, positions: &DocPositions) -> Result<DocTerms, String> {
    // (ordinal, term index, span) over every occurrence, in ordinal order.
    let mut occurrences: Vec<(u32, usize, (u32, u32))> = Vec::new();
    for (ti, ((term, _, offsets), ordinals)) in source.terms.iter().zip(positions).enumerate() {
        if term.contains(BIGRAM_SEPARATOR) {
            return Err(format!(
                "term {term:?} contains the bigram separator; the analyzer contract forbids it"
            ));
        }
        for (&span, &ordinal) in offsets.iter().zip(ordinals) {
            occurrences.push((ordinal, ti, span));
        }
    }
    occurrences.sort_unstable();
    if occurrences.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("two occurrences share one token ordinal".into());
    }
    // Group by bigram term in first-occurrence order (the same order the
    // analyzer emits terms in), one posting per distinct pair.
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut terms: DocTerms = Vec::new();
    for pair in occurrences.windows(2) {
        let (first_ordinal, first_ti, first_span) = pair[0];
        let (second_ordinal, second_ti, second_span) = pair[1];
        if second_ordinal != first_ordinal + 1 {
            continue;
        }
        let term = bigram_term(&source.terms[first_ti].0, &source.terms[second_ti].0);
        let span = (first_span.0, second_span.1);
        match index.get(&term) {
            Some(&i) => {
                terms[i].1 += 1;
                terms[i].2.push(span);
            }
            None => {
                index.insert(term.clone(), terms.len());
                terms.push((term, 1, vec![span]));
            }
        }
    }
    Ok(terms)
}
