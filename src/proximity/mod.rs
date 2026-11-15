//! Phrase and proximity search: the two
//! ingest-time payloads and the one query-time predicate behind them.
//!
//! - A **bigram column** is an ordinary derived BM25 field whose terms are
//!   adjacent-token pairs of a source field. A bigram is a term, so it
//!   costs one posting per distinct pair per document and needs no new
//!   scorer; it answers a two-term exact phrase with a single term lookup.
//! - **Token positions** are an opt-in per-field payload (one ordinal per
//!   occurrence) that answers longer phrases and slop exactly.
//! - The **ordered window** predicate reads positions per candidate at the
//!   heap gate every scorer already shares, so a phrase constraint only
//!   removes documents and every block-max bound stays a valid bound.
//!
//! Everything here is derived from token ORDINALS, never from character
//! offsets: a dropped token and a run of whitespace look identical between
//! two spans, and a phrase index built on that guess would match "new
//! york" against "new — york" and call it exact.

mod bigrams;
#[cfg(test)]
mod tests;
mod window;

use crate::postings::{AnalyzedField, DocPositions, DocTerms};

pub use bigrams::derive_bigrams;
pub use window::phrase_matches;

/// Suffix that names a source field's derived bigram column
/// (`body` -> `body.bigrams`).
pub const BIGRAM_FIELD_SUFFIX: &str = ".bigrams";

/// The field name of `source`'s bigram column.
pub fn bigram_field_name(source: &str) -> String {
    format!("{source}{BIGRAM_FIELD_SUFFIX}")
}

/// The source field a bigram column name derives from, if it is one.
pub fn bigram_source(field: &str) -> Option<&str> {
    field.strip_suffix(BIGRAM_FIELD_SUFFIX)
}

/// Separator inside a bigram term. Neither tokenizer this engine runs
/// (Unicode whitespace splitting and UAX 29 word boundaries) can put a
/// space inside a token, and no normalizer or stemmer inserts one, so the
/// pair `first SPACE second` is collision-free — which [`derive_bigrams`]
/// still checks rather than assumes.
pub const BIGRAM_SEPARATOR: char = ' ';

/// The analyzer fingerprint of a bigram column derived from a source
/// field analyzed under `source_fingerprint`: the source's identity plus
/// this derivation, so two columns whose sources were analyzed
/// differently never share a fingerprint under one name. Never 0 (the
/// "unknown" sentinel of the field-table contract).
pub fn bigram_fingerprint(source_fingerprint: u64) -> u64 {
    let mut value = source_fingerprint ^ 0x4249_4752_414d_5f31; // "BIGRAM_1"
    for byte in b"protomolt-bigram-column-v1" {
        value = (value ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    if value == 0 {
        1
    } else {
        value
    }
}

/// The bigram term for two adjacent terms.
pub fn bigram_term(first: &str, second: &str) -> String {
    let mut term = String::with_capacity(first.len() + 1 + second.len());
    term.push_str(first);
    term.push(BIGRAM_SEPARATOR);
    term.push_str(second);
    term
}

/// Which route serves a phrase request on one field, decided from the
/// fleet's declared capabilities rather than from a guess: an exact
/// two-term phrase can be one bigram term; anything else needs the
/// field's token positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhraseRoute {
    /// Serve the two-term exact phrase as a single term of the bigram
    /// column named here.
    BigramColumn(String),
    /// Serve it through the field's token positions at the heap gate.
    Positions,
}

/// Choose the route for a phrase over `sequence` (indexes into the
/// analyzed terms) with `slop`, given whether every shard serves the
/// bigram column and whether every shard carries positions for the
/// field. A sequence of one term is an ordinary term query and needs
/// neither; the caller handles that before asking.
pub fn choose_route(
    field: &str,
    sequence_len: usize,
    slop: u32,
    bigram_column_everywhere: bool,
    positions_everywhere: bool,
) -> Result<PhraseRoute, String> {
    if sequence_len == 2 && slop == 0 && bigram_column_everywhere {
        return Ok(PhraseRoute::BigramColumn(bigram_field_name(field)));
    }
    if positions_everywhere {
        return Ok(PhraseRoute::Positions);
    }
    let need = if sequence_len == 2 && slop == 0 {
        format!(
            "a bigram column ({}) or token positions on every shard",
            bigram_field_name(field)
        )
    } else if slop == 0 {
        format!("token positions on every shard (a bigram column answers only two-term phrases; this one has {sequence_len} terms)")
    } else {
        "token positions on every shard (slop needs ordinals, and a bigram column has none)"
            .to_string()
    };
    Err(format!(
        "field {field:?} cannot serve a phrase query with slop {slop} over {sequence_len} terms: it needs {need}; \
         declare it with --position-fields (or --bigram-fields) and rebuild the generation"
    ))
}

/// The query-side term sequence of a positioned analysis: term indexes in
/// token order, one per occurrence. A query analyzed under the same spec
/// as the field yields the phrase exactly as the field would index it,
/// dropped tokens included (they widen the gaps the slop must cover).
pub fn query_sequence(terms: &DocTerms, positions: &DocPositions) -> Result<Vec<usize>, String> {
    let mut ordered: Vec<(u32, usize)> = Vec::new();
    for (ti, ordinals) in positions.iter().enumerate() {
        if ordinals.len() != terms.get(ti).map_or(0, |(_, _, offsets)| offsets.len()) {
            return Err("query positions are not parallel to its occurrences".into());
        }
        for &ordinal in ordinals {
            ordered.push((ordinal, ti));
        }
    }
    ordered.sort_unstable();
    Ok(ordered.into_iter().map(|(_, ti)| ti).collect())
}

/// Complete alternatives through the query lattice, rather than all parallel
/// arcs flattened into a single phrase. Refuse excessive expansion explicitly.
pub fn query_paths(field: &AnalyzedField) -> Result<Vec<Vec<usize>>, String> {
    field.check_positions()?;
    let positions = field.positions.as_ref().ok_or("query lacks positions")?;
    if field.pos_lengths.is_none() {
        return Ok(vec![query_sequence(&field.terms, positions)?]);
    }
    let lengths = field.pos_lengths.as_ref().unwrap();
    let mut arcs: std::collections::BTreeMap<u32, Vec<(usize, u32)>> = Default::default();
    let mut count = 0;
    for (term, (positions, lengths)) in positions.iter().zip(lengths).enumerate() {
        for (&pos, &len) in positions.iter().zip(lengths) {
            let end = pos
                .checked_add(len)
                .ok_or("query lattice position overflow")?;
            arcs.entry(pos).or_default().push((term, end));
            count += 1;
            if count > 4096 {
                return Err("query lattice exceeds 4096 arcs".into());
            }
        }
    }
    let Some((&start, _)) = arcs.first_key_value() else {
        return Ok(vec![]);
    };
    let mut pending = vec![(start, Vec::new())];
    let mut paths = Vec::new();
    while let Some((cursor, path)) = pending.pop() {
        if let Some((_, choices)) = arcs.range(cursor..).next() {
            for &(term, end) in choices {
                let mut next = path.clone();
                next.push(term);
                pending.push((end, next));
            }
        } else {
            paths.push(path);
        }
        if pending.len() + paths.len() > 256 {
            return Err("query lattice exceeds 256 paths".into());
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// Validate both legacy linear phrases and bounded lattice alternatives at
/// every shard entry point before indexing a query's term vector.
pub(crate) fn validate_leg(leg: &crate::pb::PhraseLeg, terms: usize) -> Result<(), String> {
    let minimum = if leg.alternatives.is_empty() { 2 } else { 1 };
    if leg.alternatives.len() > 255
        || leg.sequence.len() < minimum
        || leg.sequence.len() > 4096
        || leg.sequence.iter().any(|&i| i as usize >= terms)
        || leg.alternatives.iter().any(|p| {
            p.terms.is_empty()
                || p.terms.len() > 4096
                || p.terms.iter().any(|&i| i as usize >= terms)
        })
    {
        return Err(format!("phrase sequence must name at least {minimum} of the query's {terms} terms; alternatives must be nonempty and bounded"));
    }
    Ok(())
}
