//! Phrase and proximity search (`docs/phrase-proximity.md`): the two
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

use crate::postings::{AnalyzedField, Bm25Index, DocPositions, DocTerms};

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
/// (Unicode whitespace splitting and UAX #29 word boundaries) can put a
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
    let length = terms.iter().map(|(_, tf, _)| *tf).sum();
    Ok(AnalyzedField {
        terms,
        length,
        positions: None,
    })
}

/// The ordered-window phrase predicate: whether `doc_id` contains the
/// term sequence `terms` (indexes into `terms`' own vocabulary via
/// `sequence`) in order, with at most `slop` intervening token positions
/// over the whole window. `slop == 0` is exact adjacency.
///
/// Exact by construction: for every occurrence of the first term as a
/// window start, the earliest later occurrence of each next term gives
/// the tightest window from that start (choosing a later occurrence can
/// only widen it), so the minimum over starts is the minimum over all
/// ordered assignments. A repeated term in the sequence is fine: each
/// step demands a strictly later ordinal, so one token never serves two
/// slots.
///
/// The field must carry positions (`Bm25Index::has_positions`); callers
/// refuse the request by name before reaching here, and this returns
/// false rather than guessing if they did not.
pub fn phrase_matches(
    index: &dyn Bm25Index,
    terms: &[String],
    sequence: &[usize],
    slop: u32,
    doc_id: u32,
) -> bool {
    if sequence.is_empty() || !index.has_positions() {
        return false;
    }
    // Positions per sequence slot; a term missing from the document ends
    // it. Terms repeated in the sequence read their positions once.
    let mut per_term: Vec<Option<Vec<u32>>> = vec![None; terms.len()];
    for &ti in sequence {
        if per_term[ti].is_none() {
            match index.posting_positions(&terms[ti], doc_id) {
                Some(positions) if !positions.is_empty() => per_term[ti] = Some(positions),
                _ => return false,
            }
        }
    }
    let slots: Vec<&[u32]> = sequence
        .iter()
        .map(|&ti| per_term[ti].as_deref().expect("filled above"))
        .collect();
    let n = slots.len() as u64;
    let budget = u64::from(slop) + n; // max allowed span = n + slop
    'starts: for &start in slots[0] {
        let mut last = start;
        for slot in &slots[1..] {
            // Earliest occurrence strictly after `last`.
            let next = match slot.binary_search(&(last + 1)) {
                Ok(i) => slot[i],
                Err(i) if i < slot.len() => slot[i],
                Err(_) => continue 'starts,
            };
            last = next;
            if u64::from(last - start) + 1 > budget {
                continue 'starts;
            }
        }
        return true;
    }
    false
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postings::{AnalyzedDoc, Bm25Store};

    fn positioned(terms: &[(&str, &[u32], &[(u32, u32)])]) -> AnalyzedField {
        AnalyzedField {
            terms: terms
                .iter()
                .map(|(t, ords, spans)| (t.to_string(), ords.len() as u32, spans.to_vec()))
                .collect(),
            length: terms.iter().map(|(_, ords, _)| ords.len() as u32).sum(),
            positions: Some(terms.iter().map(|(_, ords, _)| ords.to_vec()).collect()),
        }
    }

    #[test]
    fn bigrams_pair_only_ordinal_neighbors() {
        // "new york new jersey" with a dropped token between "new" and
        // "jersey": ordinals 0 1 2 (3 dropped) 4.
        let field = positioned(&[
            ("new", &[0, 2], &[(0, 3), (9, 12)]),
            ("york", &[1], &[(4, 8)]),
            ("jersey", &[4], &[(15, 21)]),
        ]);
        let bigrams = derive_bigrams(&field).unwrap();
        assert_eq!(
            bigrams.terms,
            vec![
                ("new york".to_string(), 1, vec![(0, 8)]),
                ("york new".to_string(), 1, vec![(4, 12)]),
            ],
            "new/jersey are not ordinal neighbors, so no bigram"
        );
        assert_eq!(bigrams.length, 2);
        assert!(bigrams.positions.is_none());
    }

    #[test]
    fn bigrams_refuse_without_positions_and_never_guess_from_spans() {
        let mut field = positioned(&[("new", &[0], &[(0, 3)]), ("york", &[1], &[(4, 8)])]);
        field.positions = None;
        let error = derive_bigrams(&field).unwrap_err();
        assert!(error.contains("requires token positions"), "{error}");
    }

    #[test]
    fn bigrams_repeat_within_a_document() {
        let field = positioned(&[
            ("hot", &[0, 2], &[(0, 3), (8, 11)]),
            ("dog", &[1, 3], &[(4, 7), (12, 15)]),
        ]);
        let bigrams = derive_bigrams(&field).unwrap();
        assert_eq!(
            bigrams.terms[0],
            ("hot dog".to_string(), 2, vec![(0, 7), (8, 15)])
        );
        assert_eq!(bigrams.terms[1], ("dog hot".to_string(), 1, vec![(4, 11)]));
    }

    fn store_with(docs: &[&[(&str, &[u32])]]) -> Bm25Store {
        let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
        for (i, doc) in docs.iter().enumerate() {
            let terms: DocTerms = doc
                .iter()
                .map(|(t, ords)| {
                    (
                        t.to_string(),
                        ords.len() as u32,
                        ords.iter().map(|&o| (o * 4, o * 4 + 3)).collect(),
                    )
                })
                .collect();
            let positions: DocPositions = doc.iter().map(|(_, ords)| ords.to_vec()).collect();
            let length = terms.iter().map(|(_, tf, _)| *tf).sum();
            store.add_document(
                i as u32,
                format!("doc {i}"),
                AnalyzedDoc::body_positioned(terms, positions, length),
            );
        }
        store
    }

    #[test]
    fn window_is_ordered_and_exact_at_every_slop() {
        // doc 0: "a b c"        doc 1: "a x b c"       doc 2: "b a c"
        // doc 3: "a b x x c"    doc 4: "a a b"
        let store = store_with(&[
            &[("a", &[0]), ("b", &[1]), ("c", &[2])],
            &[("a", &[0]), ("x", &[1]), ("b", &[2]), ("c", &[3])],
            &[("b", &[0]), ("a", &[1]), ("c", &[2])],
            &[("a", &[0]), ("b", &[1]), ("x", &[2, 3]), ("c", &[4])],
            &[("a", &[0, 1]), ("b", &[2])],
        ]);
        let terms = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let abc = [0usize, 1, 2];
        let matches = |slop: u32| -> Vec<u32> {
            (0..5)
                .filter(|&d| phrase_matches(&store, &terms, &abc, slop, d))
                .collect()
        };
        assert_eq!(matches(0), vec![0], "exact adjacency");
        assert_eq!(matches(1), vec![0, 1], "one intervening token");
        assert_eq!(matches(2), vec![0, 1, 3], "two intervening tokens");
        assert!(
            !phrase_matches(&store, &terms, &[1, 0, 2], 0, 0),
            "order matters: b a c is not a b c"
        );
        assert!(
            phrase_matches(&store, &terms, &[1, 0, 2], 0, 2),
            "b a c matches doc 2 exactly"
        );
        // A repeated term needs two distinct tokens.
        assert!(phrase_matches(&store, &terms, &[0, 0, 1], 0, 4), "a a b");
        assert!(
            !phrase_matches(&store, &terms, &[0, 0, 1], 0, 0),
            "a b c has one 'a'"
        );
        // A term absent from the document never matches.
        let terms_z = vec!["a".to_string(), "z".to_string()];
        assert!(!phrase_matches(&store, &terms_z, &[0, 1], 100, 0));
    }

    #[test]
    fn window_refuses_a_field_without_positions_instead_of_guessing() {
        let mut store = Bm25Store::with_fields(&["body"]);
        store.add_document(
            0,
            "new york".into(),
            AnalyzedDoc::body(
                vec![
                    ("new".into(), 1, vec![(0, 3)]),
                    ("york".into(), 1, vec![(4, 8)]),
                ],
                2,
            ),
        );
        let terms = vec!["new".to_string(), "york".to_string()];
        assert!(!store.has_positions());
        assert!(!phrase_matches(&store, &terms, &[0, 1], 0, 0));
    }

    #[test]
    fn route_choice_names_what_is_missing() {
        assert_eq!(
            choose_route("body", 2, 0, true, false).unwrap(),
            PhraseRoute::BigramColumn("body.bigrams".into())
        );
        assert_eq!(
            choose_route("body", 2, 0, true, true).unwrap(),
            PhraseRoute::BigramColumn("body.bigrams".into())
        );
        assert_eq!(
            choose_route("body", 2, 1, true, true).unwrap(),
            PhraseRoute::Positions
        );
        assert_eq!(
            choose_route("body", 3, 0, true, true).unwrap(),
            PhraseRoute::Positions
        );
        let error = choose_route("body", 3, 0, true, false).unwrap_err();
        assert!(error.contains("answers only two-term phrases"), "{error}");
        let error = choose_route("body", 2, 1, true, false).unwrap_err();
        assert!(error.contains("slop needs ordinals"), "{error}");
        let error = choose_route("title", 2, 0, false, false).unwrap_err();
        assert!(error.contains("title.bigrams"), "{error}");
    }

    #[test]
    fn query_sequence_follows_token_order_with_repeats() {
        let terms: DocTerms = vec![
            ("york".into(), 1, vec![(4, 8)]),
            ("new".into(), 2, vec![(0, 3), (9, 12)]),
        ];
        let positions: DocPositions = vec![vec![1], vec![0, 2]];
        assert_eq!(query_sequence(&terms, &positions).unwrap(), vec![1, 0, 1]);
    }
}
