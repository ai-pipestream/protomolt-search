//! Server-side snippets (`docs/highlighting.md`): the shard cuts each hit's
//! stored text into sentence-bounded windows around the query's
//! occurrence spans, merges overlapping spans, and returns the pieces in
//! original-text UTF-16 coordinates. Everything here is arithmetic over
//! data the shard already holds — the text, the sentence spans stored
//! at ingest, and the occurrence spans the scorer collected — so the
//! query path touches no analyzer.
//!
//! Two cuts exist and each snippet names the one it got:
//!
//! - **Sentence**: the unit is a stored sentence span. A sentence longer
//!   than `max_chars` is trimmed to a window inside it around its first
//!   highlight and reported as `TruncatedSentence`.
//! - **Window**: no sentence spans are consulted. Highlights that fit
//!   within `max_chars` of each other form one cluster, and each cluster
//!   gets a window of at most `max_chars` around it.
//!
//! A window edge lands on whitespace or the text's edge, never inside a
//! token; if honoring that would push the anchoring highlight out, the
//! window is the anchor's own whitespace-delimited run, and may then
//! exceed `max_chars`. Offsets are UTF-16 code units of the original
//! text, the unit every persisted span uses; a snippet's `text` is the
//! UTF-8 slice at those units.

use tonic::Status;

/// Snippets per hit when the request leaves `max_snippets` at 0.
pub const DEFAULT_MAX_SNIPPETS: u32 = 3;
/// Snippet width in UTF-16 units when the request leaves `max_chars` at 0.
pub const DEFAULT_MAX_CHARS: u32 = 300;
/// Smallest `max_chars` accepted: below this a window cannot hold a
/// highlight and its neighbours.
pub const MIN_MAX_CHARS: u32 = 16;
/// Largest `max_snippets` accepted per hit.
pub const MAX_SNIPPETS: u32 = 64;
/// Largest `max_chars` accepted: past this a snippet is a document.
pub const MAX_CHARS: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Sentence,
    Window,
}

/// How a snippet's bounds were chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// Both edges are a stored sentence's edges.
    Sentence,
    /// A stored sentence wider than `max_chars`, trimmed around its
    /// first highlight.
    TruncatedSentence,
    /// A window around a highlight cluster, no sentence consulted.
    Window,
}

/// A validated highlight request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub fields: Vec<String>,
    pub mode: Mode,
    pub max_snippets: usize,
    pub max_chars: u32,
}

impl Plan {
    /// Validate a wire spec. `fields` empty means the body.
    pub fn from_spec(spec: &crate::pb::HighlightSpec) -> Result<Plan, Status> {
        let mode = match crate::pb::HighlightMode::try_from(spec.mode) {
            Ok(crate::pb::HighlightMode::Unspecified) | Ok(crate::pb::HighlightMode::Sentence) => {
                Mode::Sentence
            }
            Ok(crate::pb::HighlightMode::Window) => Mode::Window,
            Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "HighlightSpec.mode {} is not a HighlightMode",
                    spec.mode
                )))
            }
        };
        let max_snippets = match spec.max_snippets {
            0 => DEFAULT_MAX_SNIPPETS,
            n if n > MAX_SNIPPETS => {
                return Err(Status::invalid_argument(format!(
                    "HighlightSpec.max_snippets {n} exceeds the maximum {MAX_SNIPPETS}"
                )))
            }
            n => n,
        };
        let max_chars = match spec.max_chars {
            0 => DEFAULT_MAX_CHARS,
            n if n < MIN_MAX_CHARS => {
                return Err(Status::invalid_argument(format!(
                    "HighlightSpec.max_chars {n} is below the minimum {MIN_MAX_CHARS}"
                )))
            }
            n if n > MAX_CHARS => {
                return Err(Status::invalid_argument(format!(
                    "HighlightSpec.max_chars {n} exceeds the maximum {MAX_CHARS}"
                )))
            }
            n => n,
        };
        let mut fields: Vec<String> = if spec.fields.is_empty() {
            vec!["body".to_string()]
        } else {
            spec.fields.clone()
        };
        for (i, field) in fields.iter().enumerate() {
            if field.is_empty() {
                return Err(Status::invalid_argument(
                    "HighlightSpec.fields: a field name is empty",
                ));
            }
            if fields[..i].contains(field) {
                return Err(Status::invalid_argument(format!(
                    "HighlightSpec.fields: {field:?} repeats"
                )));
            }
        }
        fields.dedup();
        Ok(Plan {
            fields,
            mode,
            max_snippets: max_snippets as usize,
            max_chars,
        })
    }

    pub fn wire_cut(cut: Cut) -> i32 {
        match cut {
            Cut::Sentence => crate::pb::SnippetCut::Sentence as i32,
            Cut::TruncatedSentence => crate::pb::SnippetCut::TruncatedSentence as i32,
            Cut::Window => crate::pb::SnippetCut::Window as i32,
        }
    }
}

/// One snippet of one hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snippet {
    /// UTF-16 bounds in the original text.
    pub start: u32,
    pub end: u32,
    /// The UTF-8 text at those bounds.
    pub text: String,
    /// Merged occurrence spans inside the bounds, ascending, in
    /// original-text UTF-16 units.
    pub highlights: Vec<(u32, u32)>,
    pub cut: Cut,
    /// The sentence's ordinal in the stored table for a sentence cut;
    /// `None` for a window.
    pub sentence_index: Option<usize>,
}

/// Sort spans and merge any that overlap or touch.
pub fn merge_spans(spans: &mut Vec<(u32, u32)>) {
    spans.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(spans.len());
    for &(start, end) in spans.iter() {
        match out.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => out.push((start, end)),
        }
    }
    *spans = out;
}

/// UTF-16 unit → byte offset map for one text: the boundaries of every
/// character, so a persisted UTF-16 offset resolves to a byte offset by
/// binary search, and an offset inside a surrogate pair is caught.
struct Utf16Map {
    /// `(utf16, byte)` at every character start, plus the end.
    boundaries: Vec<(u32, usize)>,
}

impl Utf16Map {
    fn new(text: &str) -> Utf16Map {
        let mut boundaries = Vec::with_capacity(text.len() + 1);
        let mut utf16 = 0u32;
        for (byte, ch) in text.char_indices() {
            boundaries.push((utf16, byte));
            utf16 += ch.len_utf16() as u32;
        }
        boundaries.push((utf16, text.len()));
        Utf16Map { boundaries }
    }

    fn len_utf16(&self) -> u32 {
        self.boundaries.last().map_or(0, |b| b.0)
    }

    fn byte_at(&self, utf16: u32) -> Result<usize, Status> {
        match self.boundaries.binary_search_by_key(&utf16, |b| b.0) {
            Ok(i) => Ok(self.boundaries[i].1),
            Err(_) => Err(Status::internal(format!(
                "highlight: UTF-16 offset {utf16} is not a character boundary of the stored \
                 text (a surrogate pair is split, or the span outruns the text)"
            ))),
        }
    }

    /// The character-boundary index of `utf16`, when it is one.
    fn index_of(&self, utf16: u32) -> Option<usize> {
        self.boundaries.binary_search_by_key(&utf16, |b| b.0).ok()
    }
}

/// Whether the character starting at boundary `i` is whitespace.
fn is_space_at(text: &str, map: &Utf16Map, i: usize) -> bool {
    let byte = map.boundaries[i].1;
    text[byte..].chars().next().is_some_and(char::is_whitespace)
}

/// A window of at most `max_chars` UTF-16 units inside `bounds` around
/// `anchor` (the first highlight), edges snapped outward-to-inward onto
/// whitespace so no token is cut. When the snap would exclude the
/// anchor, the window becomes the anchor's whitespace-delimited run.
fn window(
    text: &str,
    map: &Utf16Map,
    bounds: (u32, u32),
    anchor: (u32, u32),
    max_chars: u32,
) -> Result<(u32, u32), Status> {
    let (lo, hi) = bounds;
    // Lead with a third of the budget before the anchor, the rest after.
    let lead = max_chars / 3;
    let mut start = anchor.0.saturating_sub(lead).max(lo);
    let mut end = start.saturating_add(max_chars).min(hi);
    if end - start < max_chars {
        start = end.saturating_sub(max_chars).max(lo);
    }
    // Snap inward: the start moves forward to the first character after
    // a whitespace (or stays at the bound); the end moves back to the
    // last whitespace (or stays at the bound).
    let mut si = map
        .index_of(start)
        .ok_or_else(|| Status::internal("highlight: window start off a character boundary"))?;
    if start != lo {
        while si + 1 < map.boundaries.len()
            && map.boundaries[si].0 < hi
            && !is_space_at(text, map, si)
        {
            si += 1;
        }
        // `si` sits on a whitespace character: the token starts after it.
        while si + 1 < map.boundaries.len()
            && map.boundaries[si].0 < hi
            && is_space_at(text, map, si)
        {
            si += 1;
        }
    }
    let mut ei = map
        .index_of(end)
        .ok_or_else(|| Status::internal("highlight: window end off a character boundary"))?;
    if end != hi {
        // If the end lands mid-token (the character before it and the
        // character at it are both non-space), back up to whitespace.
        while ei > si && !is_space_at(text, map, ei) && ei > 0 && !is_space_at(text, map, ei - 1) {
            ei -= 1;
        }
        // Trim trailing whitespace inside the window.
        while ei > si && is_space_at(text, map, ei - 1) {
            ei -= 1;
        }
    }
    start = map.boundaries[si].0;
    end = map.boundaries[ei].0;
    if start <= anchor.0 && anchor.1 <= end {
        return Ok((start, end));
    }
    // The anchor did not survive the snap: take its own run instead.
    let mut ai = map
        .index_of(anchor.0)
        .ok_or_else(|| Status::internal("highlight: anchor off a character boundary"))?;
    while ai > 0 && map.boundaries[ai - 1].0 >= lo && !is_space_at(text, map, ai - 1) {
        ai -= 1;
    }
    let mut bi = map
        .index_of(anchor.1)
        .ok_or_else(|| Status::internal("highlight: anchor end off a character boundary"))?;
    while bi + 1 < map.boundaries.len() && map.boundaries[bi].0 < hi && !is_space_at(text, map, bi)
    {
        bi += 1;
    }
    Ok((map.boundaries[ai].0, map.boundaries[bi].0))
}

/// One candidate unit (a sentence or a highlight cluster) with the
/// occurrences it holds, ranked by distinct terms, then occurrences,
/// then position.
struct Unit {
    bounds: (u32, u32),
    /// `(term index, span)` inside the bounds.
    occurrences: Vec<(usize, (u32, u32))>,
    /// The stored sentence this unit is, in sentence mode.
    sentence_index: Option<usize>,
}

impl Unit {
    fn distinct_terms(&self) -> usize {
        let mut terms: Vec<usize> = self.occurrences.iter().map(|o| o.0).collect();
        terms.sort_unstable();
        terms.dedup();
        terms.len()
    }
    fn rank_key(&self) -> (std::cmp::Reverse<usize>, std::cmp::Reverse<usize>, u32) {
        (
            std::cmp::Reverse(self.distinct_terms()),
            std::cmp::Reverse(self.occurrences.len()),
            self.bounds.0,
        )
    }
}

/// Cut `max_snippets` snippets of `text` around `occurrences`
/// (`(term index, UTF-16 span)` pairs). `sentences` is the stored
/// sentence table, required in sentence mode and unread in window mode;
/// an occurrence outside every sentence is a contract break and refuses.
pub fn snippets(
    text: &str,
    sentences: Option<&[(u32, u32)]>,
    occurrences: &[(usize, (u32, u32))],
    mode: Mode,
    max_snippets: usize,
    max_chars: u32,
) -> Result<Vec<Snippet>, Status> {
    if occurrences.is_empty() || max_snippets == 0 {
        return Ok(Vec::new());
    }
    let map = Utf16Map::new(text);
    let text_end = map.len_utf16();
    for &(_, (start, end)) in occurrences {
        if start > end || end > text_end {
            return Err(Status::internal(format!(
                "highlight: occurrence [{start}, {end}) outruns the stored text of {text_end} \
                 UTF-16 units"
            )));
        }
        // Both edges must be character boundaries: an offset inside a
        // surrogate pair cannot be sliced, and cannot have come from an
        // analyzer that reported code-point-aligned spans.
        map.byte_at(start)?;
        map.byte_at(end)?;
    }
    let mut units: Vec<Unit> = match mode {
        Mode::Sentence => {
            let sentences = sentences.ok_or_else(|| {
                Status::failed_precondition(
                    "highlight: sentence mode over a field without stored sentence spans",
                )
            })?;
            let mut by_sentence: Vec<Option<Unit>> = (0..sentences.len()).map(|_| None).collect();
            for &(ti, (start, end)) in occurrences {
                // The sentence whose start is at or before this occurrence.
                let si = match sentences.binary_search_by(|s| s.0.cmp(&start)) {
                    Ok(i) => i,
                    Err(0) => {
                        return Err(Status::internal(format!(
                            "highlight: occurrence [{start}, {end}) lies before every stored \
                             sentence span"
                        )))
                    }
                    Err(i) => i - 1,
                };
                let (s_start, s_end) = sentences[si];
                if !(s_start <= start && end <= s_end) {
                    return Err(Status::internal(format!(
                        "highlight: occurrence [{start}, {end}) lies outside every stored \
                         sentence span (nearest [{s_start}, {s_end}))"
                    )));
                }
                by_sentence[si]
                    .get_or_insert_with(|| Unit {
                        bounds: (s_start, s_end),
                        occurrences: Vec::new(),
                        sentence_index: Some(si),
                    })
                    .occurrences
                    .push((ti, (start, end)));
            }
            by_sentence.into_iter().flatten().collect()
        }
        Mode::Window => {
            let mut sorted: Vec<(usize, (u32, u32))> = occurrences.to_vec();
            sorted.sort_unstable_by_key(|o| o.1);
            let mut clusters: Vec<Unit> = Vec::new();
            for (ti, span) in sorted {
                match clusters.last_mut() {
                    Some(last) if span.1.saturating_sub(last.bounds.0) <= max_chars => {
                        last.bounds.1 = last.bounds.1.max(span.1);
                        last.occurrences.push((ti, span));
                    }
                    _ => clusters.push(Unit {
                        bounds: span,
                        occurrences: vec![(ti, span)],
                        sentence_index: None,
                    }),
                }
            }
            clusters
        }
    };
    units.sort_by_key(Unit::rank_key);
    units.truncate(max_snippets);
    units.sort_by_key(|u| u.bounds.0);

    let mut out: Vec<Snippet> = Vec::with_capacity(units.len());
    for unit in units {
        let mut highlights: Vec<(u32, u32)> = unit.occurrences.iter().map(|o| o.1).collect();
        merge_spans(&mut highlights);
        let anchor = highlights[0];
        let (bounds, cut) = match mode {
            Mode::Sentence => {
                if unit.bounds.1 - unit.bounds.0 <= max_chars {
                    (unit.bounds, Cut::Sentence)
                } else {
                    (
                        window(text, &map, unit.bounds, anchor, max_chars)?,
                        Cut::TruncatedSentence,
                    )
                }
            }
            Mode::Window => (
                window(text, &map, (0, text_end), anchor, max_chars)?,
                Cut::Window,
            ),
        };
        highlights.retain(|&(s, e)| bounds.0 <= s && e <= bounds.1);
        // A window that grew over an earlier one merges into it, so the
        // response never repeats text.
        if let Some(prev) = out.last_mut() {
            if bounds.0 <= prev.end {
                prev.end = prev.end.max(bounds.1);
                prev.text = text[map.byte_at(prev.start)?..map.byte_at(prev.end)?].to_string();
                prev.highlights.extend(highlights);
                merge_spans(&mut prev.highlights);
                if prev.cut != cut {
                    prev.cut = Cut::Window;
                }
                if prev.sentence_index != unit.sentence_index {
                    prev.sentence_index = None;
                }
                continue;
            }
        }
        out.push(Snippet {
            start: bounds.0,
            end: bounds.1,
            text: text[map.byte_at(bounds.0)?..map.byte_at(bounds.1)?].to_string(),
            highlights,
            cut,
            sentence_index: unit.sentence_index,
        });
    }
    Ok(out)
}

/// Whether every occurrence lies inside one sentence of `sentences`
/// (sorted, non-overlapping): the ingest-time contract check that lets
/// the query path trust the table. Returns the first offender.
pub fn check_coverage(
    sentences: &[(u32, u32)],
    occurrences: impl Iterator<Item = (u32, u32)>,
) -> Result<(), (u32, u32)> {
    for (start, end) in occurrences {
        let covered = match sentences.binary_search_by(|s| s.0.cmp(&start)) {
            Ok(i) => end <= sentences[i].1,
            Err(0) => false,
            Err(i) => end <= sentences[i - 1].1,
        };
        if !covered {
            return Err((start, end));
        }
    }
    Ok(())
}

/// Whether a sentence table is sorted by start and non-overlapping, with
/// no empty span: the shape the writer stores and the reader trusts.
pub fn check_sentence_table(sentences: &[(u32, u32)]) -> Result<(), String> {
    let mut prev_end = 0u32;
    for (i, &(start, end)) in sentences.iter().enumerate() {
        if start >= end {
            return Err(format!(
                "sentence {i}: empty or inverted span [{start}, {end})"
            ));
        }
        if i > 0 && start < prev_end {
            return Err(format!(
                "sentence {i}: starts at {start}, before the previous sentence ends at {prev_end}"
            ));
        }
        prev_end = end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u16len(s: &str) -> u32 {
        s.encode_utf16().count() as u32
    }

    /// UTF-16 span of the `n`th occurrence of `word` in `text`.
    fn at(text: &str, word: &str, n: usize) -> (u32, u32) {
        let byte = text.match_indices(word).nth(n).expect("occurrence").0;
        let start = u16len(&text[..byte]);
        (start, start + u16len(word))
    }

    fn sentences_of(text: &str) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        let mut cursor = 0u32;
        for line in text.split('\n') {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let lead = u16len(&line[..line.find(trimmed).unwrap()]);
                out.push((cursor + lead, cursor + lead + u16len(trimmed)));
            }
            cursor += u16len(line) + 1;
        }
        out
    }

    #[test]
    fn overlapping_and_touching_spans_merge() {
        let mut spans = vec![(10, 14), (3, 5), (12, 20), (20, 22), (30, 31)];
        merge_spans(&mut spans);
        assert_eq!(spans, vec![(3, 5), (10, 22), (30, 31)]);
    }

    #[test]
    fn sentence_mode_picks_the_richest_sentences_in_text_order() {
        let text =
            "one two three\nfour court five\nsix court court seven\ncourt appeal here\nnothing";
        let sents = sentences_of(text);
        let occ = vec![
            (0, at(text, "court", 0)),
            (0, at(text, "court", 1)),
            (0, at(text, "court", 2)),
            (0, at(text, "court", 3)),
            (1, at(text, "appeal", 0)),
        ];
        let got = snippets(text, Some(&sents), &occ, Mode::Sentence, 2, 300).unwrap();
        // "court appeal here" has two distinct terms; "six court court
        // seven" has one term twice; they come back in text order.
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].text, "six court court seven");
        assert_eq!(got[0].cut, Cut::Sentence);
        assert_eq!(got[0].sentence_index, Some(2));
        assert_eq!(got[1].sentence_index, Some(3));
        assert_eq!(
            got[0].highlights,
            vec![at(text, "court", 1), at(text, "court", 2)]
        );
        assert_eq!(got[1].text, "court appeal here");
        assert_eq!(
            got[1].highlights,
            vec![at(text, "court", 3), at(text, "appeal", 0)],
            "a space keeps neighbouring highlights apart"
        );
        assert_eq!((got[1].start, got[1].end), sents[3]);
        let one = snippets(text, Some(&sents), &occ, Mode::Sentence, 1, 300).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].text, "court appeal here");
    }

    #[test]
    fn a_long_sentence_is_cut_at_whitespace_around_its_first_highlight() {
        let mut text = String::new();
        for i in 0..40 {
            text.push_str(&format!("w{i:02}abcdef "));
        }
        text.push_str("court");
        for i in 0..40 {
            text.push_str(&format!(" x{i:02}abcdef"));
        }
        let sents = vec![(0, u16len(&text))];
        let occ = vec![(0, at(&text, "court", 0))];
        let got = snippets(&text, Some(&sents), &occ, Mode::Sentence, 3, 64).unwrap();
        assert_eq!(got.len(), 1);
        let s = &got[0];
        assert_eq!(s.cut, Cut::TruncatedSentence);
        assert!(s.end - s.start <= 64, "{}", s.end - s.start);
        assert!(s.text.contains("court"));
        // Whole tokens only: the slice starts and ends at token edges.
        assert!(!s.text.starts_with(' ') && !s.text.ends_with(' '));
        let before = &text[..text.find(&s.text).unwrap()];
        assert!(before.is_empty() || before.ends_with(' '));
        let after = &text[text.find(&s.text).unwrap() + s.text.len()..];
        assert!(after.is_empty() || after.starts_with(' '));
        assert_eq!(s.highlights, vec![at(&text, "court", 0)]);
    }

    #[test]
    fn window_mode_ignores_sentences_and_clusters_nearby_highlights() {
        let text = "alpha court beta\ngamma delta court epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega appeal";
        let occ = vec![
            (0, at(text, "court", 0)),
            (0, at(text, "court", 1)),
            (1, at(text, "appeal", 0)),
        ];
        let got = snippets(text, None, &occ, Mode::Window, 3, 40).unwrap();
        assert!(got.iter().all(|s| s.cut == Cut::Window));
        assert!(got.iter().all(|s| s.sentence_index.is_none()));
        // The two courts sit within 40 units of each other: one window;
        // the appeal is far away: another.
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(got[0].text.contains("alpha court beta"));
        assert_eq!(got[0].highlights.len(), 2);
        assert!(got[1].text.ends_with("appeal"));
        assert!(got[1].end - got[1].start <= 40);
        // Sentence mode without a table refuses by name.
        let error = snippets(text, None, &occ, Mode::Sentence, 3, 40).unwrap_err();
        assert!(error.message().contains("without stored sentence spans"));
    }

    #[test]
    fn offsets_stay_utf16_and_slices_stay_on_character_boundaries() {
        let text = "😀 café court\nnaïve 𝔘nicode court";
        let sents = sentences_of(text);
        let occ = vec![(0, at(text, "court", 0)), (0, at(text, "court", 1))];
        let got = snippets(text, Some(&sents), &occ, Mode::Sentence, 3, 300).unwrap();
        assert_eq!(got.len(), 2);
        // 😀 is two units, é one, 𝔘 two: the offsets count UTF-16 units of
        // the original text, never bytes or characters.
        assert_eq!(got[0].text, "😀 café court");
        assert_eq!((got[0].start, got[0].end), (0, 13));
        assert_eq!(got[0].highlights, vec![(8, 13)]);
        assert_eq!(got[1].text, "naïve 𝔘nicode court");
        assert_eq!((got[1].start, got[1].end), (14, 34));
        assert_eq!(got[1].highlights, vec![(29, 34)]);
        // A span that splits the surrogate pair is refused, not sliced.
        let bad = vec![(0, (1, 2))];
        let error = snippets(text, None, &bad, Mode::Window, 3, 300).unwrap_err();
        assert!(error.message().contains("not a character boundary"));
        // A window cut near the emoji still lands on a boundary, and the
        // text is exactly the UTF-16 slice at the reported offsets.
        let win = snippets(text, None, &occ, Mode::Window, 3, 16).unwrap();
        assert!(!win.is_empty());
        for s in &win {
            let units: Vec<u16> = text
                .encode_utf16()
                .skip(s.start as usize)
                .take((s.end - s.start) as usize)
                .collect();
            assert_eq!(s.text, String::from_utf16(&units).unwrap());
        }
    }

    #[test]
    fn an_occurrence_outside_every_sentence_is_a_contract_break() {
        let text = "one\ntwo court";
        let sents = vec![(0, 3)];
        let occ = vec![(0, at(text, "court", 0))];
        let error = snippets(text, Some(&sents), &occ, Mode::Sentence, 3, 300).unwrap_err();
        assert!(error
            .message()
            .contains("outside every stored sentence span"));
        assert_eq!(
            check_coverage(&sents, occ.iter().map(|o| o.1)),
            Err(at(text, "court", 0))
        );
        assert_eq!(
            check_coverage(&[(0, 3), (4, 13)], occ.iter().map(|o| o.1)),
            Ok(())
        );
        assert!(check_sentence_table(&[(0, 3), (4, 13)]).is_ok());
        assert!(check_sentence_table(&[(0, 3), (2, 13)]).is_err());
        assert!(check_sentence_table(&[(3, 3)]).is_err());
        let overrun = vec![(0, (0, 99))];
        let error = snippets(text, None, &overrun, Mode::Window, 3, 300).unwrap_err();
        assert!(error.message().contains("outruns the stored text"));
    }

    #[test]
    fn a_highlight_wider_than_the_budget_keeps_its_whole_token() {
        let long = "x".repeat(50);
        let text = format!("start {long} end");
        let occ = vec![(0, at(&text, &long, 0))];
        let got = snippets(&text, None, &occ, Mode::Window, 1, 16).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].text, long);
        assert_eq!(got[0].highlights, vec![at(&text, &long, 0)]);
    }
}
