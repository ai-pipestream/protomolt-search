//! Query-time synonyms and did-you-mean (`docs/synonyms.md`).
//!
//! A synonym rule is a line of words: symmetric (every word expands to
//! every other) or one-way (`terms` expand to `to`). Rules are written
//! as surface words and analyzed under the field's analysis spec at
//! expansion time, so a rule matches the stems the dictionary holds;
//! the analyzed forms are cached per spec. An expansion adds terms to
//! the query, each scored as the ordinary term it is, and is reported
//! back by term. Nothing is rewritten at ingest and no posting changes.
//!
//! Did-you-mean ranks dictionary terms within an edit bound of a query
//! term by the optimal string alignment distance (Damerau-Levenshtein
//! with adjacent transpositions), hand-rolled here.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use serde::Deserialize;
use tonic::Status;

use crate::pb::{AnalysisSpec, SynonymRule};

/// A synonym table as loaded from TOML (`--synonyms=<file>`):
///
/// ```toml
/// [[rules]]
/// terms = ["car", "automobile", "motor vehicle"]
///
/// [[rules]]
/// terms = ["nyc"]
/// to = ["new york city"]
/// ```
#[derive(Debug, Default, Deserialize)]
struct SynonymFile {
    #[serde(default)]
    rules: Vec<SynonymFileRule>,
}

#[derive(Debug, Default, Deserialize)]
struct SynonymFileRule {
    #[serde(default)]
    terms: Vec<String>,
    #[serde(default)]
    to: Vec<String>,
}

/// One rule after analysis: the analyzed forms of its entries. An entry
/// that analyzes to several terms (a phrase) matches nothing as a query
/// term but contributes every term of it as an expansion.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AnalyzedRule {
    /// Each `terms` entry as its analyzed term list.
    terms: Vec<Vec<String>>,
    /// Each `to` entry as its analyzed term list; empty for a symmetric
    /// rule.
    to: Vec<Vec<String>>,
}

/// The coordinator's synonym table plus the per-spec analysis cache.
#[derive(Debug, Default)]
pub struct SynonymTable {
    rules: Vec<SynonymRule>,
    analyzed: Mutex<HashMap<String, Vec<AnalyzedRule>>>,
}

impl SynonymTable {
    pub fn from_rules(rules: Vec<SynonymRule>) -> Result<Self, String> {
        for (i, rule) in rules.iter().enumerate() {
            validate_rule(rule).map_err(|e| format!("synonym rule {}: {e}", i + 1))?;
        }
        Ok(SynonymTable {
            rules,
            analyzed: Mutex::new(HashMap::new()),
        })
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read synonyms {}: {e}", path.display()))?;
        let file: SynonymFile =
            toml::from_str(&text).map_err(|e| format!("parse synonyms {}: {e}", path.display()))?;
        Self::from_rules(
            file.rules
                .into_iter()
                .map(|r| SynonymRule {
                    terms: r.terms,
                    to: r.to,
                })
                .collect(),
        )
        .map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn rules(&self) -> &[SynonymRule] {
        &self.rules
    }
}

/// A rule names at least two words when symmetric, or at least one on
/// each side when one-way; no entry is blank.
pub fn validate_rule(rule: &SynonymRule) -> Result<(), String> {
    if rule
        .terms
        .iter()
        .chain(&rule.to)
        .any(|w| w.trim().is_empty())
    {
        return Err("an empty entry".to_string());
    }
    if rule.to.is_empty() {
        if rule.terms.len() < 2 {
            return Err("a symmetric rule needs at least two entries".to_string());
        }
    } else if rule.terms.is_empty() {
        return Err("a one-way rule needs at least one entry on the left".to_string());
    }
    Ok(())
}

/// The analyzed forms of `rules` under `spec`, through `analyze` (one
/// entry per call). A cache keyed by the spec serves the coordinator's
/// table; request rules are analyzed each time.
async fn analyze_rules<F, Fut>(
    rules: &[SynonymRule],
    spec: Option<&AnalysisSpec>,
    analyze: F,
) -> Result<Vec<AnalyzedRule>, Status>
where
    F: Fn(String, Option<AnalysisSpec>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<String>, Status>>,
{
    let mut out = Vec::with_capacity(rules.len());
    for rule in rules {
        let mut terms = Vec::with_capacity(rule.terms.len());
        for entry in &rule.terms {
            terms.push(analyze(entry.clone(), spec.cloned()).await?);
        }
        let mut to = Vec::with_capacity(rule.to.len());
        for entry in &rule.to {
            to.push(analyze(entry.clone(), spec.cloned()).await?);
        }
        out.push(AnalyzedRule { terms, to });
    }
    Ok(out)
}

fn spec_key(spec: Option<&AnalysisSpec>) -> String {
    format!("{spec:?}")
}

/// Expand `terms` (the analyzed query terms of one field) under the
/// table (unless `table_off`) and the request's own rules: every added
/// term is appended once, and the expansions are reported per matched
/// query term in query order. A query term matches a rule entry that
/// analyzed to exactly that one term; a symmetric rule adds the other
/// entries' terms, a one-way rule adds its `to` entries' terms.
pub async fn expand<F, Fut>(
    table: Option<&SynonymTable>,
    table_off: bool,
    request_rules: &[SynonymRule],
    field: &str,
    spec: Option<&AnalysisSpec>,
    terms: &mut Vec<String>,
    analyze: F,
) -> Result<Vec<crate::pb::SynonymExpansion>, Status>
where
    F: Fn(String, Option<AnalysisSpec>) -> Fut + Clone,
    Fut: std::future::Future<Output = Result<Vec<String>, Status>>,
{
    for (i, rule) in request_rules.iter().enumerate() {
        validate_rule(rule)
            .map_err(|e| Status::invalid_argument(format!("synonym rule {}: {e}", i + 1)))?;
    }
    let mut analyzed: Vec<AnalyzedRule> = Vec::new();
    if let (Some(table), false) = (table, table_off) {
        if !table.is_empty() {
            let key = spec_key(spec);
            let cached = table
                .analyzed
                .lock()
                .expect("synonym cache lock")
                .get(&key)
                .cloned();
            let rules = match cached {
                Some(rules) => rules,
                None => {
                    let rules = analyze_rules(&table.rules, spec, analyze.clone()).await?;
                    table
                        .analyzed
                        .lock()
                        .expect("synonym cache lock")
                        .insert(key, rules.clone());
                    rules
                }
            };
            analyzed.extend(rules);
        }
    }
    if !request_rules.is_empty() {
        analyzed.extend(analyze_rules(request_rules, spec, analyze).await?);
    }
    if analyzed.is_empty() || terms.is_empty() {
        return Ok(Vec::new());
    }
    let query: Vec<String> = terms.clone();
    let mut expansions = Vec::new();
    for term in &query {
        let mut added: Vec<String> = Vec::new();
        for rule in &analyzed {
            let matched = rule
                .terms
                .iter()
                .any(|entry| entry.len() == 1 && entry[0] == *term);
            if !matched {
                continue;
            }
            let sources: Vec<&Vec<String>> = if rule.to.is_empty() {
                rule.terms
                    .iter()
                    .filter(|entry| !(entry.len() == 1 && entry[0] == *term))
                    .collect()
            } else {
                rule.to.iter().collect()
            };
            for entry in sources {
                for t in entry {
                    if t != term && !added.contains(t) {
                        added.push(t.clone());
                    }
                }
            }
        }
        if added.is_empty() {
            continue;
        }
        for t in &added {
            if !terms.contains(t) {
                terms.push(t.clone());
            }
        }
        expansions.push(crate::pb::SynonymExpansion {
            field: field.to_string(),
            term: term.clone(),
            terms: added,
        });
    }
    Ok(expansions)
}

/// Optimal string alignment distance over chars: insert, delete,
/// substitute, and adjacent transposition, each one edit. `bound` cuts
/// the computation short: the result is exact when at most `bound`,
/// and `bound + 1` otherwise.
pub fn edit_distance(a: &str, b: &str, bound: usize) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > bound {
        return bound + 1;
    }
    let width = b.len() + 1;
    let mut prev2: Vec<usize> = Vec::new();
    let mut prev: Vec<usize> = (0..width).collect();
    for i in 1..=a.len() {
        let mut cur = vec![0usize; width];
        cur[0] = i;
        let mut row_min = cur[0];
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut best = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                best = best.min(prev2[j - 2] + 1);
            }
            cur[j] = best;
            row_min = row_min.min(best);
        }
        if row_min > bound {
            return bound + 1;
        }
        prev2 = prev;
        prev = cur;
    }
    prev[b.len()].min(bound + 1)
}

/// Rank the dictionary entries (term, summed df, shards) within
/// `max_edits` of `term`, best first: distance ascending, df
/// descending, term bytes ascending; the term itself is excluded.
pub fn rank_candidates(
    term: &str,
    entries: &BTreeMap<String, (u64, u32)>,
    max_edits: usize,
    limit: usize,
) -> Vec<crate::pb::TermCandidate> {
    let mut ranked: Vec<(usize, u64, &String, u32)> = entries
        .iter()
        .filter(|(candidate, _)| candidate.as_str() != term)
        .filter_map(|(candidate, (df, shards))| {
            let d = edit_distance(term, candidate, max_edits);
            (d <= max_edits).then_some((d, *df, candidate, *shards))
        })
        .collect();
    ranked.sort_by(|x, y| {
        x.0.cmp(&y.0)
            .then_with(|| y.1.cmp(&x.1))
            .then_with(|| x.2.as_bytes().cmp(y.2.as_bytes()))
    });
    ranked.truncate(limit);
    ranked
        .into_iter()
        .map(|(distance, df, term, shards)| crate::pb::TermCandidate {
            term: term.clone(),
            df,
            distance: distance as u32,
            shards,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_distance_counts_transpositions_as_one_and_respects_the_bound() {
        assert_eq!(edit_distance("court", "court", 2), 0);
        assert_eq!(edit_distance("court", "cuort", 2), 1, "a transposition");
        assert_eq!(edit_distance("court", "cort", 2), 1, "a deletion");
        assert_eq!(edit_distance("court", "courts", 2), 1, "an insertion");
        assert_eq!(edit_distance("court", "coart", 2), 1, "a substitution");
        assert_eq!(edit_distance("court", "cuorts", 2), 2);
        assert_eq!(
            edit_distance("court", "xxxxx", 2),
            3,
            "past the bound: bound + 1"
        );
        assert_eq!(edit_distance("", "ab", 1), 2);
        assert_eq!(edit_distance("naïve", "naive", 1), 1, "chars, not bytes");
    }

    #[test]
    fn candidates_rank_by_distance_then_df_then_term() {
        let mut entries = BTreeMap::new();
        entries.insert("court".to_string(), (50, 2));
        entries.insert("courts".to_string(), (7, 1));
        entries.insert("cort".to_string(), (7, 1));
        entries.insert("coupon".to_string(), (90, 2));
        entries.insert("cour".to_string(), (1, 1));
        let ranked = rank_candidates("cuort", &entries, 2, 10);
        let got: Vec<(&str, u32)> = ranked
            .iter()
            .map(|c| (c.term.as_str(), c.distance))
            .collect();
        // "cort" is one deletion away, like the transposition "court";
        // "courts" (a transposition and an insertion) and "cour" (two
        // deletions) are two, and df orders them.
        assert_eq!(
            got,
            vec![("court", 1), ("cort", 1), ("courts", 2), ("cour", 2)],
            "distance first, then df, then the term"
        );
        assert!(rank_candidates("court", &entries, 1, 10)
            .iter()
            .all(|c| c.term != "court"));
    }

    #[test]
    fn rules_validate_by_shape() {
        assert!(validate_rule(&SynonymRule {
            terms: vec!["car".into()],
            to: vec![],
        })
        .is_err());
        assert!(validate_rule(&SynonymRule {
            terms: vec![],
            to: vec!["x".into()],
        })
        .is_err());
        assert!(validate_rule(&SynonymRule {
            terms: vec!["a".into(), " ".into()],
            to: vec![],
        })
        .is_err());
        assert!(validate_rule(&SynonymRule {
            terms: vec!["car".into(), "automobile".into()],
            to: vec![],
        })
        .is_ok());
    }
}
