//! Glossaries: compiled phrase vocabularies matched over analyzed text.

use std::fmt;

use super::*;

/// Compiled, cross-platform phrase vocabulary.
///
/// Matching is Unicode case-folded when requested, requires Unicode word
/// boundaries, and can expose either leftmost-longest non-overlapping matches
/// or every explicitly registered overlapping concept for phrase indexing.
#[derive(Clone)]
pub struct Glossary {
    pub(crate) entries: Vec<GlossaryEntry>,
    token_counts: Vec<u32>,
    matcher: AhoCorasick,
    /// Entry indexes for each distinct matcher pattern. More than one concept
    /// may intentionally register the same normalized surface form.
    pattern_entries: Vec<Vec<usize>>,
    ignore_case: bool,
    fingerprint: u64,
}

impl fmt::Debug for Glossary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Glossary")
            .field("entries", &self.entries)
            .field("ignore_case", &self.ignore_case)
            .field("fingerprint", &format_args!("{:016x}", self.fingerprint))
            .finish_non_exhaustive()
    }
}

impl Glossary {
    /// Compile a vocabulary. Exact duplicate `(id, term)` pairs, empty values,
    /// and punctuation-only terms are rejected so the fingerprint and posting
    /// identity remain unambiguous.
    pub fn new(entries: Vec<GlossaryEntry>, ignore_case: bool) -> Result<Self, AnalysisError> {
        if entries.is_empty() {
            return Err(AnalysisError::new("glossary contains no entries"));
        }
        let mut seen = HashSet::new();
        let mut patterns = Vec::<String>::with_capacity(entries.len());
        let mut pattern_indexes = HashMap::<String, usize>::new();
        let mut pattern_entries = Vec::<Vec<usize>>::new();
        let mut token_counts = Vec::with_capacity(entries.len());
        for (entry_index, entry) in entries.iter().enumerate() {
            if entry.id.is_empty() || entry.term.is_empty() {
                return Err(AnalysisError::new(
                    "glossary ids and registered terms must not be empty",
                ));
            }
            if !seen.insert((entry.id.clone(), entry.term.clone())) {
                return Err(AnalysisError::new(format!(
                    "duplicate glossary entry ({:?}, {:?})",
                    entry.id, entry.term
                )));
            }
            let pattern = if ignore_case {
                full_case_fold(&entry.term)
            } else {
                entry.term.clone()
            };
            if pattern.is_empty() {
                return Err(AnalysisError::new(format!(
                    "glossary term {:?} is empty after normalization",
                    entry.term
                )));
            }
            let count = tokenize_uax29(&entry.term, OffsetUnit::Utf8Bytes).len() as u32;
            if count == 0 {
                return Err(AnalysisError::new(format!(
                    "glossary term {:?} contains no indexable UAX #29 token",
                    entry.term
                )));
            }
            if let Some(&pattern_index) = pattern_indexes.get(&pattern) {
                pattern_entries[pattern_index].push(entry_index);
            } else {
                let pattern_index = patterns.len();
                pattern_indexes.insert(pattern.clone(), pattern_index);
                patterns.push(pattern);
                pattern_entries.push(vec![entry_index]);
            }
            token_counts.push(count);
        }
        let matcher = AhoCorasickBuilder::new()
            .match_kind(MatchKind::Standard)
            .build(&patterns)
            .map_err(|error| AnalysisError::new(format!("invalid glossary: {error}")))?;
        let fingerprint = glossary_fingerprint(&entries, ignore_case);
        Ok(Self {
            entries,
            token_counts,
            matcher,
            pattern_entries,
            ignore_case,
            fingerprint,
        })
    }

    /// Stable, entry-order-independent vocabulary identity.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Leftmost-longest non-overlapping matches for annotation consumers.
    pub fn matches(&self, text: &str) -> Vec<GlossaryMatch> {
        self.matches_with_offset_unit(text, OffsetUnit::Utf8Bytes)
    }

    /// Leftmost-longest matches using the requested original-text coordinate
    /// system.
    pub fn matches_with_offset_unit(
        &self,
        text: &str,
        offset_unit: OffsetUnit,
    ) -> Vec<GlossaryMatch> {
        let mut matches = self.all_valid_matches(text, offset_unit);
        matches.sort_by(|a, b| {
            a.span
                .start
                .cmp(&b.span.start)
                .then_with(|| b.span.end.cmp(&a.span.end))
                .then_with(|| a.id.cmp(&b.id))
                .then_with(|| a.registered_term.cmp(&b.registered_term))
        });
        let mut end = 0;
        matches
            .into_iter()
            .filter(|candidate| {
                if candidate.span.start < end {
                    false
                } else {
                    end = candidate.span.end;
                    true
                }
            })
            .collect()
    }

    /// Every explicitly registered occurrence, including nested concepts such
    /// as both `new york` and `new york city`. No unregistered shingle is
    /// synthesized.
    pub fn index_matches(&self, text: &str) -> Vec<GlossaryMatch> {
        self.index_matches_with_offset_unit(text, OffsetUnit::Utf8Bytes)
    }

    /// Every registered occurrence using the requested original-text
    /// coordinate system.
    pub fn index_matches_with_offset_unit(
        &self,
        text: &str,
        offset_unit: OffsetUnit,
    ) -> Vec<GlossaryMatch> {
        let mut matches = self.all_valid_matches(text, offset_unit);
        matches.sort_by(|a, b| {
            a.span
                .start
                .cmp(&b.span.start)
                .then_with(|| b.span.end.cmp(&a.span.end))
                .then_with(|| a.id.cmp(&b.id))
                .then_with(|| a.registered_term.cmp(&b.registered_term))
        });
        matches
    }

    fn all_valid_matches(&self, text: &str, offset_unit: OffsetUnit) -> Vec<GlossaryMatch> {
        let folded = FoldedText::new(text, self.ignore_case);
        let mut matches = Vec::new();
        for matched in self.matcher.find_overlapping_iter(&folded.text) {
            let Some((start_utf16, start_byte)) = folded.original_boundary(matched.start()) else {
                continue;
            };
            let Some((end_utf16, end_byte)) = folded.original_boundary(matched.end()) else {
                continue;
            };
            if !is_word_boundary(text, start_byte) || !is_word_boundary(text, end_byte) {
                continue;
            }
            for &entry_index in &self.pattern_entries[matched.pattern().as_usize()] {
                let entry = &self.entries[entry_index];
                matches.push(GlossaryMatch {
                    id: entry.id.clone(),
                    registered_term: entry.term.clone(),
                    span: Span {
                        start: offset_unit.position(start_utf16, start_byte),
                        end: offset_unit.position(end_utf16, end_byte),
                    },
                    token_count: self.token_counts[entry_index],
                });
            }
        }
        matches
    }
}

/// Collision-free posting identity for a glossary concept id.
pub fn phrase_posting_term(id: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(8 + id.len() * 2);
    out.push_str("$phrase:");
    for byte in id.bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) struct FoldedText {
    text: String,
    /// A folded UTF-8 byte boundary is mapped only when it is also an original
    /// scalar boundary. This prevents a one-character pattern from matching
    /// half of an expanding fold such as `ß -> ss`.
    boundaries: Vec<Option<(u32, usize)>>,
}

impl FoldedText {
    fn new(text: &str, fold: bool) -> Self {
        if !fold {
            let mut boundaries = vec![None; text.len() + 1];
            let mut utf16 = 0u32;
            boundaries[0] = Some((0, 0));
            for (byte, ch) in text.char_indices() {
                boundaries[byte] = Some((utf16, byte));
                utf16 += ch.len_utf16() as u32;
                let end = byte + ch.len_utf8();
                boundaries[end] = Some((utf16, end));
            }
            return Self {
                text: text.to_string(),
                boundaries,
            };
        }

        let mut folded = String::with_capacity(text.len());
        let mut boundaries = vec![Some((0, 0))];
        let mut utf16 = 0u32;
        for (original_byte, ch) in text.char_indices() {
            let start = folded.len();
            let value = full_case_fold(ch.encode_utf8(&mut [0; 4]));
            folded.push_str(&value);
            boundaries.resize(folded.len() + 1, None);
            boundaries[start] = Some((utf16, original_byte));
            utf16 += ch.len_utf16() as u32;
            boundaries[folded.len()] = Some((utf16, original_byte + ch.len_utf8()));
        }
        Self {
            text: folded,
            boundaries,
        }
    }

    fn original_boundary(&self, folded_byte: usize) -> Option<(u32, usize)> {
        self.boundaries.get(folded_byte).copied().flatten()
    }
}

pub(crate) fn is_word_boundary(text: &str, byte: usize) -> bool {
    let left = text[..byte].chars().next_back();
    let right = text[byte..].chars().next();
    !left.is_some_and(is_word_char) || !right.is_some_and(is_word_char)
}

pub(crate) fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

pub(crate) fn glossary_fingerprint(entries: &[GlossaryEntry], ignore_case: bool) -> u64 {
    let mut canonical = entries
        .iter()
        .map(|entry| (entry.id.as_bytes(), entry.term.as_bytes()))
        .collect::<Vec<_>>();
    canonical.sort_unstable();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in b"protomolt-glossary-v1" {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    hash = (hash ^ u64::from(ignore_case)).wrapping_mul(0x100000001b3);
    for (id, term) in canonical {
        for bytes in [id, term] {
            for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
                hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
    }
    hash
}
