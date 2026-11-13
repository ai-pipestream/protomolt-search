//! One-pass `AnalyzeQuery` filter binding (`Money`, `Quantity`, `Temporal`,
//! `Place`, `Entity` -> native CEL / `FilterExpr`), `#94` filter-selectivity
//! HNSW budget selection, `MappedSurfaceLexicon` stem-to-surface completion,
//! and `QuerySpellchecker` contextual multi-word spellcheck (`#104`).

use std::collections::BTreeMap;

use tonic::Status;

use super::{NlpColumnSpec, MICROS_PER_SECOND};
use crate::pb::{FilterExpr, Suggestion, SynonymExpansion, TermCandidate, TermSuggestion};

/// HNSW filtered search execution strategy (`#94`, `#104`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FilteredAnnStrategy {
    /// Graph traversal with escalated `ef_search` proportional to `1 / selectivity`.
    Graph { ef_search: u32 },
    /// Exact prefiltered distance scan when survivor cardinality is small enough
    /// that brute-force scoring over admitted slots beats sparse graph hops.
    PrefilteredExact,
}

/// Selects the filtered HNSW visit budget (`ef_search` vs prefiltered exact scan)
/// from `base_ef`, `k`, `shard_docs`, and `estimated_selectivity` in `(0.0, 1.0]`
/// (`#94`, `#104`).
pub fn select_filtered_ann_strategy(
    base_ef: u32,
    k: u32,
    shard_docs: u64,
    estimated_selectivity: f64,
) -> FilteredAnnStrategy {
    let eff_base = base_ef.max(k).max(16);
    let sel = if !estimated_selectivity.is_finite() {
        1.0
    } else {
        estimated_selectivity.clamp(0.0, 1.0)
    };
    if sel >= 0.999 {
        return FilteredAnnStrategy::Graph {
            ef_search: eff_base,
        };
    }
    let expected_survivors = (shard_docs as f64 * sel).ceil() as u64;
    // Below ~256 expected survivors or < 1% selectivity on medium shards,
    // exact prefiltered evaluation guarantees 100% recall in fewer dot products.
    if expected_survivors <= u64::from(k.saturating_mul(8).max(128)) || sel <= 0.005 {
        return FilteredAnnStrategy::PrefilteredExact;
    }
    let scaled = ((f64::from(eff_base) / sel.max(0.02)).ceil() as u32).clamp(eff_base, 2048);
    FilteredAnnStrategy::Graph { ef_search: scaled }
}

/// Bound query execution plan produced from [`turbo_client::OnePassQueryPlan`].
#[derive(Clone, Debug)]
pub struct BoundOnePassQuery {
    /// Residual non-filter query text suitable for BM25 / neural encoding.
    pub residual_text: String,
    /// Ordinal-preserving residual query terms `(term, ordinal)`.
    pub residual_terms: Vec<(String, u32)>,
    /// Multi-word and single-word synonym expansions matched by `turbo-matcher`.
    pub synonym_expansions: Vec<SynonymExpansion>,
    /// Synthesized CEL predicate string for extracted structured filters (`""` when none).
    pub synthesized_cel: String,
    /// Conjoined native [`FilterExpr`] combining `caller_cel` and `synthesized_cel`.
    pub filter_expr: Option<FilterExpr>,
    /// Heuristic selectivity estimate in `(0.0, 1.0]` for `#94` HNSW budget escalation.
    pub estimated_selectivity: f64,
}

fn escape_cel_string(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn format_f64_cel(val: f64) -> String {
    let s = format!("{val}");
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

fn comparison_cel(lhs: &str, op: turbo_client::QueryComparisonOp, rhs: &str) -> String {
    use turbo_client::QueryComparisonOp as Op;
    let sym = match op {
        Op::Eq | Op::Range => "==",
        Op::Gt => ">",
        Op::Gte => ">=",
        Op::Lt => "<",
        Op::Lte => "<=",
    };
    format!("{lhs} {sym} {rhs}")
}

/// Translates a [`turbo_client::OnePassQueryPlan`] and [`NlpColumnSpec`] into a
/// native [`BoundOnePassQuery`] whose structured filters compile through
/// [`crate::cel::compile_filter`] onto the shard predicate IR (`#104`, `#2339`).
pub fn bind_one_pass_query(
    plan: &turbo_client::OnePassQueryPlan,
    columns: &NlpColumnSpec,
    caller_cel: Option<&str>,
) -> Result<BoundOnePassQuery, Status> {
    use turbo_client::{QueryComparisonOp as Op, StructuredFilterPayload as P};

    let mut clauses: Vec<String> = Vec::new();
    let mut selectivity = 1.0_f64;

    for f in &plan.filters {
        match &f.payload {
            P::Money {
                raw,
                amount_f64,
                ecb_converted,
            } => {
                if !columns.money_column.is_empty() {
                    let (ccy, val) = match ecb_converted {
                        Some(conv) if conv.amount_f64.is_finite() => {
                            (conv.currency.as_str().to_string(), conv.amount_f64)
                        }
                        _ => (raw.currency.as_str().to_string(), *amount_f64),
                    };
                    if val.is_finite() && !ccy.is_empty() {
                        let lhs = format!("{}[{}]", columns.money_column, escape_cel_string(&ccy));
                        clauses.push(comparison_cel(&lhs, f.comparison, &format_f64_cel(val)));
                        selectivity *= 0.25;
                    }
                }
            }
            P::Quantity {
                si_base_unit,
                si_value_f64,
                ..
            } => {
                if !columns.quantity_column.is_empty()
                    && si_value_f64.is_finite()
                    && !si_base_unit.is_empty()
                {
                    let lhs = format!(
                        "{}[{}]",
                        columns.quantity_column,
                        escape_cel_string(si_base_unit)
                    );
                    clauses.push(comparison_cel(
                        &lhs,
                        f.comparison,
                        &format_f64_cel(*si_value_f64),
                    ));
                    selectivity *= 0.25;
                }
            }
            P::Temporal(t) => {
                if !columns.date_column.is_empty() {
                    let col = &columns.date_column;
                    match (f.comparison, t.start_epoch_seconds, t.end_epoch_seconds) {
                        (Op::Gt, Some(s), Some(e)) => {
                            clauses
                                .push(format!("{col} >= {}", e.saturating_mul(MICROS_PER_SECOND)));
                            let _ = s;
                        }
                        (Op::Gte, Some(s), _) => {
                            clauses
                                .push(format!("{col} >= {}", s.saturating_mul(MICROS_PER_SECOND)));
                        }
                        (Op::Lt, Some(s), _) => {
                            clauses
                                .push(format!("{col} < {}", s.saturating_mul(MICROS_PER_SECOND)));
                        }
                        (Op::Lte, _, Some(e)) => {
                            clauses
                                .push(format!("{col} < {}", e.saturating_mul(MICROS_PER_SECOND)));
                        }
                        (_, Some(s), Some(e)) if s < e => {
                            clauses.push(format!(
                                "({col} >= {} && {col} < {})",
                                s.saturating_mul(MICROS_PER_SECOND),
                                e.saturating_mul(MICROS_PER_SECOND)
                            ));
                        }
                        (_, Some(s), None) => {
                            clauses
                                .push(format!("{col} == {}", s.saturating_mul(MICROS_PER_SECOND)));
                        }
                        _ => {}
                    }
                    selectivity *= 0.20;
                }
            }
            P::Place {
                raw, spine_path, ..
            } => {
                if !columns.geo_spine_column.is_empty() {
                    let target = if !spine_path.is_empty() {
                        spine_path.as_str()
                    } else if !raw.canonical_name.is_empty() {
                        raw.canonical_name.as_str()
                    } else {
                        ""
                    };
                    if !target.is_empty() {
                        clauses.push(format!(
                            "{} in {}",
                            escape_cel_string(target),
                            columns.geo_spine_column
                        ));
                        selectivity *= 0.15;
                    }
                }
            }
            P::Entity {
                raw,
                kb_id,
                canonical_name,
                ..
            } => {
                if !columns.affirmed_entity_column.is_empty() {
                    let canonical = kb_id
                        .as_deref()
                        .filter(|s| !s.is_empty())
                        .or_else(|| canonical_name.as_deref().filter(|s| !s.is_empty()))
                        .unwrap_or(f.surface.as_str());
                    if !canonical.is_empty() {
                        let key = format!("id:{canonical}");
                        clauses.push(format!(
                            "{} in {}",
                            escape_cel_string(&key),
                            columns.affirmed_entity_column
                        ));
                        let _ = raw;
                        selectivity *= 0.10;
                    }
                }
            }
            P::Glossary(_) => {}
        }
    }

    let synthesized_cel = clauses.join(" && ");
    let combined_cel = match (
        caller_cel.map(str::trim).filter(|s| !s.is_empty()),
        synthesized_cel.as_str(),
    ) {
        (Some(caller), "") => caller.to_string(),
        (None, synth) => synth.to_string(),
        (Some(caller), synth) => format!("({caller}) && ({synth})"),
    };
    let filter_expr = crate::cel::compile_filter(&combined_cel)?;

    let mut synonym_expansions = Vec::new();
    for fp in &plan.fields {
        for syn in &fp.synonyms {
            if !syn.terms.is_empty() {
                synonym_expansions.push(SynonymExpansion {
                    field: fp.field.clone(),
                    term: syn.surface.clone(),
                    terms: syn.terms.clone(),
                });
            }
        }
    }

    let residual_terms: Vec<(String, u32)> = plan
        .residual_terms
        .iter()
        .map(|t| (t.term.clone(), t.ordinal))
        .collect();
    let residual_text = if !plan.free_text.trim().is_empty() {
        plan.free_text.clone()
    } else {
        plan.query.clone()
    };

    Ok(BoundOnePassQuery {
        residual_text,
        residual_terms,
        synonym_expansions,
        synthesized_cel,
        filter_expr,
        estimated_selectivity: selectivity,
    })
}

/// Owned or OS-memory-mapped `TSUGLEX1` surface lexicon (`#104`, `#2341`) with
/// guaranteed 8-byte alignment so [`turbo_client::MappedSurfaceLexicon`] can
/// borrow its `u32` FST/posting tables with zero heap allocation on lookup.
#[derive(Debug)]
pub enum SharedSurfaceLexicon {
    /// Zero-copy OS file mapping (`memmap2::Mmap` is page-aligned).
    Mmap(memmap2::Mmap),
    /// 8-byte-aligned heap buffer for in-memory builders and tests.
    Aligned { words: Vec<u64>, len: usize },
}

impl SharedSurfaceLexicon {
    /// Memory-maps a `TSUGLEX1` file from disk and validates its header/bounds.
    pub fn open_mmap(path: impl AsRef<std::path::Path>) -> Result<Self, Status> {
        let file = std::fs::File::open(path.as_ref()).map_err(|e| {
            Status::invalid_argument(format!(
                "open surface lexicon {}: {e}",
                path.as_ref().display()
            ))
        })?;
        // SAFETY: read-only mapping of an immutable `TSUGLEX1` lexicon artifact.
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            Status::invalid_argument(format!(
                "mmap surface lexicon {}: {e}",
                path.as_ref().display()
            ))
        })?;
        turbo_client::MappedSurfaceLexicon::from_bytes(&mmap).map_err(|e| {
            Status::invalid_argument(format!("invalid TSUGLEX1 surface lexicon: {e}"))
        })?;
        Ok(Self::Mmap(mmap))
    }

    /// Copies `bytes` into an 8-byte-aligned buffer and validates its `TSUGLEX1` layout.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        for (w, chunk) in words.iter_mut().zip(bytes.chunks(8)) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            *w = u64::from_ne_bytes(buf);
        }
        let this = Self::Aligned {
            words,
            len: bytes.len(),
        };
        turbo_client::MappedSurfaceLexicon::from_bytes(this.as_bytes()).map_err(|e| {
            Status::invalid_argument(format!("invalid TSUGLEX1 surface lexicon: {e}"))
        })?;
        Ok(this)
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Mmap(map) => map,
            Self::Aligned { words, len } => {
                // SAFETY: `words` holds at least `len` initialized bytes and outlives `&self`.
                unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), *len) }
            }
        }
    }

    /// Returns a zero-copy [`turbo_client::MappedSurfaceLexicon`] view over this image.
    pub fn view(&self) -> turbo_client::MappedSurfaceLexicon<'_> {
        turbo_client::MappedSurfaceLexicon::from_bytes(self.as_bytes())
            .expect("validated at SharedSurfaceLexicon construction")
    }
}

/// Rewrites stemmed dictionary prefix matches into genuine surface completions
/// using [`turbo_client::MappedSurfaceLexicon`] (`#104`, `#2341`).
///
/// Preserves shard presence metadata when a surface's stem also matched on shards,
/// and surfaces lexicon prefix completions whose stems were truncated shorter
/// than the user's typed prefix (for example typing `"internationa"` when the
/// Porter stem in `.bm25` is `"intern"`).
pub fn resolve_stemmed_suggestions(
    lexicon: &turbo_client::MappedSurfaceLexicon,
    normalized_prefix: &str,
    shard_union: &BTreeMap<String, (u64, u32)>,
    limit: usize,
) -> Vec<Suggestion> {
    let mut merged: BTreeMap<String, (u64, u32)> = BTreeMap::new();

    // 1. Direct zero-copy prefix completions from the mapped surface FST.
    for comp in lexicon.suggest_prefix(normalized_prefix, limit.saturating_mul(4).max(32)) {
        let shards = shard_union.get(comp.stem).map_or(1, |&(_, s)| s.max(1));
        let df = shard_union
            .get(comp.stem)
            .map_or(comp.count, |&(d, _)| d.max(comp.count));
        merged.insert(comp.surface.to_string(), (df, shards));
    }

    // 2. Map any shard stem hits through `stem_surfaces` when the surface starts with `normalized_prefix`.
    for (stem, &(df, shards)) in shard_union {
        let surfaces = lexicon.stem_surfaces(stem, 16);
        let mut matched_surface = false;
        for s in surfaces {
            if s.surface.starts_with(normalized_prefix) {
                matched_surface = true;
                merged
                    .entry(s.surface.to_string())
                    .and_modify(|e| {
                        e.0 = e.0.max(s.count.min(df).max(1));
                        e.1 = e.1.max(shards);
                    })
                    .or_insert((s.count.max(1), shards));
            }
        }
        if !matched_surface && stem.starts_with(normalized_prefix) {
            merged.entry(stem.clone()).or_insert((df, shards));
        }
    }

    let mut ranked: Vec<(String, (u64, u32))> = merged.into_iter().collect();
    ranked.sort_by(|a, b| b.1 .0.cmp(&a.1 .0).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(limit);
    ranked
        .into_iter()
        .map(|(term, (df, shards))| Suggestion { term, df, shards })
        .collect()
}

/// Evaluates contextual multi-word bigram spellcheck (`#104`, `#2341`), returning
/// [`TermSuggestion`] entries for corrected tokens in query order.
pub fn spellcheck_query_terms(
    checker: &turbo_client::QuerySpellchecker,
    query_text: &str,
    limit: usize,
) -> Option<(String, Vec<TermSuggestion>)> {
    let res = checker.check_query(query_text, &[]);
    if !res.did_change {
        return None;
    }
    let mut out = Vec::with_capacity(res.edits.len());
    for edit in res.edits {
        let mut candidates = vec![TermCandidate {
            term: edit.replacement.clone(),
            df: 1,
            distance: edit.edit_distance,
            shards: 1,
        }];
        candidates.truncate(limit.max(1));
        out.push(TermSuggestion {
            term: edit.original,
            df: 0,
            candidates,
            dictionary_terms_scanned: 1,
        });
    }
    Some((res.corrected_query, out))
}
