//! Product-owned glossary phrase indexing.
//!
//! The portable matcher lives in `protomolt-analyzer`; this module owns the
//! server concerns around it: loading a versioned vocabulary, assigning the
//! dedicated BM25 field, deriving durable postings, and materializing entity
//! map keys. Ordinary body terms are never replaced.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use protomolt_analyzer::{phrase_posting_term, Glossary, GlossaryEntry};

use crate::pb::{MapFacetEntry, OffsetSpan, PhrasePosting};

/// A compiled phrase vocabulary and its product field mapping.
#[derive(Debug, Clone)]
pub struct PhraseIndex {
    glossary: Glossary,
    phrase_field: String,
    entity_map_field: Option<String>,
    include_ner: bool,
}

impl PhraseIndex {
    /// Load `concept-id<TAB>surface-form` entries. Blank lines and lines whose
    /// first non-space character is `#` are ignored; aliases repeat an id.
    pub fn load_tsv(
        path: &Path,
        phrase_field: String,
        entity_map_field: Option<String>,
        ignore_case: bool,
        include_ner: bool,
    ) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("read phrase glossary {}: {error}", path.display()))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| format!("phrase glossary {} is not UTF-8: {error}", path.display()))?;
        let mut entries = Vec::new();
        for (line_index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (id, term) = line.split_once('\t').ok_or_else(|| {
                format!(
                    "phrase glossary {} line {} must be concept-id<TAB>surface-form",
                    path.display(),
                    line_index + 1
                )
            })?;
            let id = id.trim();
            let term = term.trim();
            if id.is_empty() || term.is_empty() {
                return Err(format!(
                    "phrase glossary {} line {} has an empty id or surface form",
                    path.display(),
                    line_index + 1
                ));
            }
            entries.push(GlossaryEntry {
                id: id.to_string(),
                term: term.to_string(),
            });
        }
        Self::new(
            entries,
            phrase_field,
            entity_map_field,
            ignore_case,
            include_ner,
        )
    }

    pub fn new(
        entries: Vec<GlossaryEntry>,
        phrase_field: String,
        entity_map_field: Option<String>,
        ignore_case: bool,
        include_ner: bool,
    ) -> Result<Self, String> {
        if phrase_field.is_empty() || phrase_field == "body" {
            return Err("phrase field must be non-empty and different from body".to_string());
        }
        if include_ner && entity_map_field.is_none() {
            return Err("phrase NER requires an entity map field".to_string());
        }
        let glossary = Glossary::new(entries, ignore_case).map_err(|error| error.to_string())?;
        Ok(Self {
            glossary,
            phrase_field,
            entity_map_field,
            include_ner,
        })
    }

    pub fn phrase_field(&self) -> &str {
        &self.phrase_field
    }

    pub fn entity_map_field(&self) -> Option<&str> {
        self.entity_map_field.as_deref()
    }

    pub fn include_ner(&self) -> bool {
        self.include_ner
    }

    /// Analyzer identity for the dedicated phrase field. Zero remains
    /// reserved for unknown, matching the rest of the BM25 field contract.
    pub fn fingerprint(&self) -> u64 {
        let mut value = self.glossary.fingerprint() ^ 0x5052_4153_455f_5631;
        let mut eat = |bytes: &[u8]| {
            for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
                value = (value ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        };
        eat(self.phrase_field.as_bytes());
        eat(self.entity_map_field.as_deref().unwrap_or("").as_bytes());
        eat(&[u8::from(self.include_ner)]);
        if value == 0 {
            1
        } else {
            value
        }
    }

    /// Group every registered match by canonical concept posting. Nested
    /// concepts remain separate; aliases of one id share a term.
    pub fn postings(&self, text: &str) -> Vec<PhrasePosting> {
        let mut grouped = BTreeMap::<(String, String), (u32, BTreeSet<(u32, u32)>)>::new();
        for found in self.glossary.index_matches(text) {
            let term = phrase_posting_term(&found.id);
            let entry = grouped.entry((term, found.id)).or_default();
            entry.0 = entry.0.max(found.token_count);
            entry.1.insert((found.span.start, found.span.end));
        }
        grouped
            .into_iter()
            .map(
                |((term, concept_id), (token_count, offsets))| PhrasePosting {
                    term,
                    concept_id,
                    token_count,
                    offsets: offsets
                        .into_iter()
                        .map(|(start, end)| OffsetSpan { start, end })
                        .collect(),
                    field: self.phrase_field.clone(),
                },
            )
            .collect()
    }

    /// Query concepts and their token counts, one per canonical id. The
    /// longest registered alias wins if the same id appears more than once.
    pub fn query_terms(&self, text: &str) -> Vec<(String, u32)> {
        let mut terms = BTreeMap::<String, u32>::new();
        for found in self.glossary.index_matches(text) {
            terms
                .entry(phrase_posting_term(&found.id))
                .and_modify(|count| *count = (*count).max(found.token_count))
                .or_insert(found.token_count);
        }
        terms.into_iter().collect()
    }

    /// Entity map entries for every glossary concept present in a document.
    /// A concept appears once regardless of aliases or repeated mentions.
    pub fn glossary_entities(&self, postings: &[PhrasePosting]) -> Vec<MapFacetEntry> {
        let Some(field) = &self.entity_map_field else {
            return Vec::new();
        };
        postings
            .iter()
            .map(|posting| MapFacetEntry {
                field: field.clone(),
                key: entity_key("glossary", &posting.concept_id),
                value: "matched".to_string(),
            })
            .collect()
    }
}

/// Collision-free map key for a namespace and arbitrary Unicode identity.
pub fn entity_key(namespace: &str, identity: &str) -> String {
    fn push_component(out: &mut String, value: &str) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in value.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                out.push(byte as char);
            } else {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    let mut out = String::with_capacity(namespace.len() + identity.len() + 8);
    push_component(&mut out, namespace);
    out.push(':');
    push_component(&mut out, identity);
    out
}

/// Convert a durable posting list into the positional analyzed field shape.
pub fn analyzed_field(postings: &[PhrasePosting]) -> crate::postings::AnalyzedField {
    crate::postings::AnalyzedField {
        terms: postings
            .iter()
            .map(|posting| {
                (
                    posting.term.clone(),
                    posting.offsets.len() as u32,
                    posting
                        .offsets
                        .iter()
                        .map(|span| (span.start, span.end))
                        .collect(),
                )
            })
            .collect(),
        // One concept occurrence is one field token. This keeps BM25 length
        // normalization independent of the number of words in its alias;
        // token_count is a query-time evidence weight instead.
        length: postings
            .iter()
            .map(|posting| posting.offsets.len() as u32)
            .sum(),
    }
}

/// NER entity data retained from the OpenNLP analysis pass until it is
/// materialized into the configured map column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocEntity {
    pub kind: String,
    pub text: String,
    pub start: u32,
    pub end: u32,
}

/// Materialize distinct NER identities as entity-map entries. Surface text is
/// Unicode case-folded by the portable analyzer's glossary semantics only for
/// glossary matching; NER model output is kept exactly as returned here.
pub fn ner_entities(field: &str, entities: &[DocEntity]) -> Vec<MapFacetEntry> {
    let mut seen = BTreeSet::new();
    entities
        .iter()
        .filter_map(|entity| {
            let identity = format!(
                "{}\0{}",
                protomolt_analyzer::unicode_case_fold(&entity.kind),
                protomolt_analyzer::unicode_case_fold(&entity.text)
            );
            let key = entity_key("ner", &identity);
            seen.insert(key.clone()).then(|| MapFacetEntry {
                field: field.to_string(),
                key,
                value: "matched".to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> PhraseIndex {
        PhraseIndex::new(
            vec![
                GlossaryEntry {
                    id: "nyc".into(),
                    term: "New York City".into(),
                },
                GlossaryEntry {
                    id: "new-york".into(),
                    term: "New York".into(),
                },
                GlossaryEntry {
                    id: "hot-dog".into(),
                    term: "Hot Dog".into(),
                },
            ],
            "phrases".into(),
            Some("entities".into()),
            true,
            false,
        )
        .unwrap()
    }

    #[test]
    fn derives_nested_registered_postings_without_arbitrary_shingles() {
        let postings = index().postings("😀 New York City hot dog");
        assert_eq!(postings.len(), 3);
        assert!(postings.iter().any(|value| value.concept_id == "nyc"));
        assert!(postings.iter().any(|value| value.concept_id == "new-york"));
        assert!(!postings.iter().any(|value| value.concept_id == "york-city"));
        let nyc = postings
            .iter()
            .find(|value| value.concept_id == "nyc")
            .unwrap();
        assert_eq!(nyc.offsets, [OffsetSpan { start: 3, end: 16 }]);
    }

    #[test]
    fn entity_map_deduplicates_concepts_and_ner_identities() {
        let index = index();
        let postings = index.postings("New York City and New York City");
        let glossary = index.glossary_entities(&postings);
        assert_eq!(glossary.len(), 2);
        let entities = vec![
            DocEntity {
                kind: "LOCATION".into(),
                text: "New York City".into(),
                start: 0,
                end: 13,
            },
            DocEntity {
                kind: "LOCATION".into(),
                text: "New York City".into(),
                start: 18,
                end: 31,
            },
        ];
        assert_eq!(ner_entities("entities", &entities).len(), 1);
    }
}
