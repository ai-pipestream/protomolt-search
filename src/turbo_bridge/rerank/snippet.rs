//! Wire snippet construction and the served order of reranked hits.

use crate::pb::{OffsetSpan, Snippet, SnippetCut};

#[cfg(any(feature = "net", feature = "local-core"))]
pub(super) fn sort_reranked_hits(hits: &mut [super::RerankedHit]) {
    hits.sort_by(|a, b| {
        b.calibrated_probability
            .total_cmp(&a.calibrated_probability)
            .then_with(|| b.raw_logit.total_cmp(&a.raw_logit))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
}

fn snap_char_bounds(text: &str, start: u32, end: u32) -> Option<(usize, usize)> {
    if text.is_empty() {
        return None;
    }
    let s = text.floor_char_boundary((start as usize).min(text.len()));
    let e = text.ceil_char_boundary((end as usize).min(text.len()));
    (s < e).then_some((s, e))
}

pub(super) fn build_wire_snippet(
    field: &str,
    text: &str,
    sentence_spans: &[(u32, u32)],
    raw_start: u32,
    raw_end: u32,
    lineage_shift: u32,
) -> Option<Snippet> {
    let (s, e) = snap_char_bounds(text, raw_start, raw_end)?;
    let slice = &text[s..e];
    let s_u32 = s as u32;
    let e_u32 = e as u32;
    let sentence_index = sentence_spans
        .iter()
        .position(|&(ss, se)| ss == s_u32 && se == e_u32)
        .map(|i| i as u32);
    let cut = if sentence_index.is_some() {
        SnippetCut::Sentence as i32
    } else {
        SnippetCut::Window as i32
    };
    let shifted_start = s_u32.saturating_add(lineage_shift);
    let shifted_end = e_u32.saturating_add(lineage_shift);
    Some(Snippet {
        field: field.to_string(),
        text: slice.to_string(),
        start: shifted_start,
        end: shifted_end,
        highlights: vec![OffsetSpan {
            start: shifted_start,
            end: shifted_end,
        }],
        cut,
        sentence_index,
    })
}
