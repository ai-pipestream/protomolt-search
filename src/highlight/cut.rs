//! Snippet selection: candidate units, their ranking, and the cut of
//! each chosen unit.

use tonic::Status;

use super::boundary::{window, CharBoundaryMap};
use super::{merge_spans, Cut, Mode, Snippet};

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
/// (`(term index, UTF-8 byte span)` pairs). `sentences` is the stored
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
    let map = CharBoundaryMap::new(text);
    validate_occurrences(&map, occurrences)?;
    let mut units = match mode {
        Mode::Sentence => sentence_units(sentences, occurrences)?,
        Mode::Window => cluster_units(occurrences, max_chars),
    };
    units.sort_by_key(Unit::rank_key);
    units.truncate(max_snippets);
    units.sort_by_key(|u| u.bounds.0);

    let mut out: Vec<Snippet> = Vec::with_capacity(units.len());
    for unit in units {
        let mut highlights: Vec<(u32, u32)> = unit.occurrences.iter().map(|o| o.1).collect();
        merge_spans(&mut highlights);
        let anchor = highlights[0];
        let (bounds, cut) = cut_bounds(text, &map, mode, unit.bounds, anchor, max_chars)?;
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

/// Refuses an occurrence that outruns the text or has an edge off a
/// character boundary.
fn validate_occurrences(
    map: &CharBoundaryMap,
    occurrences: &[(usize, (u32, u32))],
) -> Result<(), Status> {
    let text_end = map.len_bytes();
    for &(_, (start, end)) in occurrences {
        if start > end || end > text_end {
            return Err(Status::internal(format!(
                "highlight: occurrence [{start}, {end}) outruns the stored text of {text_end} \
             UTF-8 bytes"
            )));
        }
        // Both edges must be character boundaries: an offset inside a
        // multi-byte scalar cannot be sliced, and cannot have come from an
        // analyzer that reported code-point-aligned spans.
        map.byte_at(start)?;
        map.byte_at(end)?;
    }
    Ok(())
}

/// One unit per stored sentence that holds an occurrence, in table order.
fn sentence_units(
    sentences: Option<&[(u32, u32)]>,
    occurrences: &[(usize, (u32, u32))],
) -> Result<Vec<Unit>, Status> {
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
    Ok(by_sentence.into_iter().flatten().collect())
}

/// One unit per cluster of occurrences that fit within `max_chars` of
/// the cluster's start, in text order.
fn cluster_units(occurrences: &[(usize, (u32, u32))], max_chars: u32) -> Vec<Unit> {
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

/// The bounds of one unit's snippet and the cut that chose them.
fn cut_bounds(
    text: &str,
    map: &CharBoundaryMap,
    mode: Mode,
    unit_bounds: (u32, u32),
    anchor: (u32, u32),
    max_chars: u32,
) -> Result<((u32, u32), Cut), Status> {
    Ok(match mode {
        Mode::Sentence => {
            if unit_bounds.1 - unit_bounds.0 <= max_chars {
                (unit_bounds, Cut::Sentence)
            } else {
                (
                    window(text, map, unit_bounds, anchor, max_chars)?,
                    Cut::TruncatedSentence,
                )
            }
        }
        Mode::Window => (
            window(text, map, (0, map.len_bytes()), anchor, max_chars)?,
            Cut::Window,
        ),
    })
}
