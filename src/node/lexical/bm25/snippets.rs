//! Highlight plans and snippet cutting from stored text.

use crate::node::*;

/// Validate highlight fields and the sentence table required by sentence mode.
pub(in crate::node) fn highlight_plan(
    store: &Bm25Shard,
    spec: Option<&crate::pb::HighlightSpec>,
) -> Result<Option<crate::highlight::Plan>, Status> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let plan = crate::highlight::Plan::from_spec(spec)?;
    let body = store.field_name(0);
    for field in &plan.fields {
        if field != body {
            return Err(Status::invalid_argument(format!(
                "highlight field {field:?}: snippets are cut from stored text, and only the \
                 body's text ({body:?}) is stored on this shard"
            )));
        }
    }
    if plan.mode == crate::highlight::Mode::Sentence && !store.field_has_sentences(0) {
        return Err(Status::failed_precondition(format!(
            "field {body:?} on this shard stores no sentence spans, so sentence-mode snippets \
             cannot be cut; ingest with --sentence-fields={body}, or ask for \
             HIGHLIGHT_MODE_WINDOW, which cuts at whitespace without them"
        )));
    }
    Ok(Some(plan))
}

/// Cut snippets from stored text and scored offsets without rerunning analysis.
pub(in crate::node) fn cut_snippets(
    plan: &crate::highlight::Plan,
    index: &dyn Bm25Index,
    field: &str,
    local: u32,
    occurrences: &[(usize, (u32, u32))],
) -> Result<Vec<crate::pb::Snippet>, Status> {
    if occurrences.is_empty() {
        return Ok(Vec::new());
    }
    let text = index.text(local).ok_or_else(|| {
        Status::internal(format!(
            "highlight: hit {local} has no stored text to cut snippets from"
        ))
    })?;
    let sentences = match plan.mode {
        crate::highlight::Mode::Sentence => index.doc_sentences(local),
        crate::highlight::Mode::Window => None,
    };
    let cut = crate::highlight::snippets(
        &text,
        sentences.as_deref(),
        occurrences,
        plan.mode,
        plan.max_snippets,
        plan.max_chars,
    )?;
    Ok(cut
        .into_iter()
        .map(|s| crate::pb::Snippet {
            field: field.to_string(),
            text: s.text,
            start: s.start,
            end: s.end,
            highlights: s
                .highlights
                .into_iter()
                .map(|(start, end)| OffsetSpan { start, end })
                .collect(),
            cut: crate::highlight::Plan::wire_cut(s.cut),
            sentence_index: s.sentence_index.map(|i| i as u32),
        })
        .collect())
}
