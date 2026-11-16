//! Server-side snippets: the shard cuts each hit's
//! stored text into sentence-bounded windows around the query's
//! occurrence spans, merges overlapping spans, and returns the pieces in
//! original-text UTF-8 byte coordinates. Everything here is arithmetic over
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
//! exceed `max_chars`. Offsets are UTF-8 byte offsets of the original
//! text, the unit every persisted span uses; a snippet's `text` is the
//! UTF-8 slice at those offsets.

#![deny(clippy::result_large_err)]

use std::sync::{Mutex, OnceLock};

mod boundary;
mod cut;
#[cfg(test)]
mod tests;

pub use cut::snippets;

/// Snippets per hit when the request leaves `max_snippets` at 0.
pub const DEFAULT_MAX_SNIPPETS: u32 = 3;
/// Snippet width in UTF-8 bytes when the request leaves `max_chars` at 0.
pub const DEFAULT_MAX_CHARS: u32 = 300;
/// Smallest `max_chars` accepted: below this a window cannot hold a
/// highlight and its neighbours.
pub const MIN_MAX_CHARS: u32 = 16;
/// Largest `max_snippets` accepted per hit.
pub const MAX_SNIPPETS: u32 = 64;
/// Largest `max_chars` accepted: past this a snippet is a document.
pub const MAX_CHARS: u32 = 4096;

/// A compact domain error, converted to transport status only at the edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HighlightError {
    InvalidArgument(CompactMessage),
    Internal(CompactMessage),
    FailedPrecondition(CompactMessage),
}

#[derive(Debug)]
pub struct CompactMessage(u16);

#[derive(Default)]
struct MessageRegistry {
    next_id: u16,
    messages: std::collections::HashMap<u16, Box<str>>,
}

impl MessageRegistry {
    fn shared() -> &'static Mutex<Self> {
        static REGISTRY: OnceLock<Mutex<MessageRegistry>> = OnceLock::new();
        REGISTRY.get_or_init(|| {
            Mutex::new(MessageRegistry {
                next_id: 1,
                messages: std::collections::HashMap::new(),
            })
        })
    }
}

impl CompactMessage {
    fn new(text: String) -> Self {
        let mut registry = MessageRegistry::shared()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let id = (0..=u16::MAX)
            .map(|offset| registry.next_id.wrapping_add(offset))
            .find(|id| !registry.messages.contains_key(id))
            .expect("too many live highlight errors");
        registry.next_id = id.wrapping_add(1);
        registry.messages.insert(id, text.into_boxed_str());
        Self(id)
    }

    fn message(&self) -> String {
        MessageRegistry::shared()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .messages
            .get(&self.0)
            .expect("highlight message is registered")
            .to_string()
    }
}

impl Clone for CompactMessage {
    fn clone(&self) -> Self {
        Self::new(self.message())
    }
}

impl PartialEq for CompactMessage {
    fn eq(&self, other: &Self) -> bool {
        self.message() == other.message()
    }
}

impl Eq for CompactMessage {}

impl Drop for CompactMessage {
    fn drop(&mut self) {
        MessageRegistry::shared()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .messages
            .remove(&self.0);
    }
}

impl HighlightError {
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(CompactMessage::new(message.into()))
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(CompactMessage::new(message.into()))
    }

    pub(crate) fn failed_precondition(message: impl Into<String>) -> Self {
        Self::FailedPrecondition(CompactMessage::new(message.into()))
    }

    pub fn message(&self) -> String {
        match self {
            Self::InvalidArgument(message)
            | Self::Internal(message)
            | Self::FailedPrecondition(message) => message.message(),
        }
    }
}

impl std::fmt::Display for HighlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for HighlightError {}

impl From<HighlightError> for tonic::Status {
    fn from(error: HighlightError) -> Self {
        let message = error.message();
        match error {
            HighlightError::InvalidArgument(_) => Self::invalid_argument(message),
            HighlightError::Internal(_) => Self::internal(message),
            HighlightError::FailedPrecondition(_) => Self::failed_precondition(message),
        }
    }
}

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
    pub fn from_spec(spec: &crate::pb::HighlightSpec) -> Result<Plan, HighlightError> {
        let mode = match crate::pb::HighlightMode::try_from(spec.mode) {
            Ok(crate::pb::HighlightMode::Unspecified) | Ok(crate::pb::HighlightMode::Sentence) => {
                Mode::Sentence
            }
            Ok(crate::pb::HighlightMode::Window) => Mode::Window,
            Err(_) => {
                return Err(HighlightError::invalid_argument(format!(
                    "HighlightSpec.mode {} is not a HighlightMode",
                    spec.mode
                )))
            }
        };
        let max_snippets = match spec.max_snippets {
            0 => DEFAULT_MAX_SNIPPETS,
            n if n > MAX_SNIPPETS => {
                return Err(HighlightError::invalid_argument(format!(
                    "HighlightSpec.max_snippets {n} exceeds the maximum {MAX_SNIPPETS}"
                )))
            }
            n => n,
        };
        let max_chars = match spec.max_chars {
            0 => DEFAULT_MAX_CHARS,
            n if n < MIN_MAX_CHARS => {
                return Err(HighlightError::invalid_argument(format!(
                    "HighlightSpec.max_chars {n} is below the minimum {MIN_MAX_CHARS}"
                )))
            }
            n if n > MAX_CHARS => {
                return Err(HighlightError::invalid_argument(format!(
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
                return Err(HighlightError::invalid_argument(
                    "HighlightSpec.fields: a field name is empty",
                ));
            }
            if fields[..i].contains(field) {
                return Err(HighlightError::invalid_argument(format!(
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
    /// UTF-8 byte bounds in the original text.
    pub start: u32,
    pub end: u32,
    /// The UTF-8 text at those bounds.
    pub text: String,
    /// Merged occurrence spans inside the bounds, ascending, in
    /// original-text UTF-8 bytes.
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
