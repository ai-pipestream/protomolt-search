//! Positioned query analysis and per-field phrase-route planning.

use super::lexical_context::FusedLexicalQuery;
use super::*;

struct AnalyzedField {
    terms: Vec<String>,
    phrase: Option<PhraseConstraint>,
}
struct PhraseConstraint {
    sequence: Vec<usize>,
    slop: u32,
    alternatives: Vec<Vec<usize>>,
}
pub(super) struct FusedAnalysis {
    fields: Vec<AnalyzedField>,
}
impl FusedAnalysis {
    pub(super) fn is_empty(&self) -> bool {
        self.fields.iter().all(|field| field.terms.is_empty())
    }
}

impl CoordinatorServiceImpl {
    pub(super) async fn analyze_fused_query(
        &self,
        query: &FusedLexicalQuery<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<FusedAnalysis, Status> {
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let mut fields = Vec::with_capacity(query.fields().len());
        for field in query.fields() {
            let analyzed = analyze_query(
                &addr,
                &query.input().text,
                field.analysis.as_ref(),
                &field.prefixes,
            )
            .await?;
            let mut analyzed = positioned_field(field, analyzed)?;
            expansions.prefixes.extend(
                self.expand_prefixes(
                    &field.field,
                    field.analysis.as_ref(),
                    &field.prefixes,
                    &mut analyzed.terms,
                )
                .await?,
            );
            expansions.synonyms.extend(
                self.expand_synonyms(
                    &field.field,
                    field.analysis.as_ref(),
                    &field.synonyms,
                    field.synonyms_off,
                    &mut analyzed.terms,
                )
                .await?,
            );
            fields.push(analyzed);
        }
        Ok(FusedAnalysis { fields })
    }
}

fn positioned_field(
    field: &crate::pb::QueryField,
    analyzed: crate::postings::AnalyzedField,
) -> Result<AnalyzedField, Status> {
    let mut terms = Vec::new();
    let mut remap = Vec::with_capacity(analyzed.terms.len());
    for (term, _, _) in &analyzed.terms {
        let index = match terms.iter().position(|seen| seen == term) {
            Some(index) => index,
            None => {
                terms.push(term.clone());
                terms.len() - 1
            }
        };
        remap.push(index);
    }
    let phrase = if let Some(constraint) = &field.phrase {
        if terms.is_empty() {
            return Err(Status::invalid_argument(format!(
                "field {:?}: the phrase text analyzed to no terms; there is no window to match",
                field.field
            )));
        }
        if analyzed.positions.is_none() {
            return Err(Status::failed_precondition(format!("field {:?}: the query analysis carried no token positions, so the phrase's token order cannot be established; the analysis backend must return its token layer", field.field)));
        }
        let paths = crate::proximity::query_paths(&analyzed)
            .map_err(|error| Status::invalid_argument(format!("query positions: {error}")))?;
        let mut paths = paths.into_iter().map(|path| {
            path.into_iter()
                .map(|index| remap[index])
                .collect::<Vec<_>>()
        });
        let sequence = paths
            .next()
            .ok_or_else(|| Status::invalid_argument("empty query lattice"))?;
        Some(PhraseConstraint {
            sequence,
            slop: constraint.slop,
            alternatives: paths.collect(),
        })
    } else {
        None
    };
    Ok(AnalyzedField { terms, phrase })
}

pub(super) struct FusedProbe {
    analyzed: FusedAnalysis,
    stats_fields: Vec<crate::pb::FieldTerms>,
    bigram_probe: Vec<Option<usize>>,
    probe_from: usize,
}

impl FusedProbe {
    pub(super) fn new(
        coordinator: &CoordinatorServiceImpl,
        query: &FusedLexicalQuery<'_>,
        analyzed: FusedAnalysis,
    ) -> Self {
        let fields = query.fields();
        let mut stats_fields: Vec<_> = fields
            .iter()
            .zip(&analyzed.fields)
            .map(|(field, analysis)| crate::pb::FieldTerms {
                field: field.field.clone(),
                terms: analysis.terms.clone(),
            })
            .collect();
        let probe_from = stats_fields.len();
        let mut bigram_probe = vec![None; fields.len()];
        for (index, field) in fields.iter().enumerate() {
            let Some(phrase) = &analyzed.fields[index].phrase else {
                continue;
            };
            if phrase.slop != 0 || phrase.sequence.len() != 2 || !phrase.alternatives.is_empty() {
                continue;
            }
            let column = crate::proximity::bigram_field_name(&field.field);
            if coordinator
                .field_permissions
                .as_ref()
                .is_some_and(|permissions| {
                    !permissions.can_use(&column)
                        || (query.input().explain && !permissions.can_disclose(&column))
                })
            {
                continue;
            }
            let terms = &analyzed.fields[index].terms;
            let bigram = crate::proximity::bigram_term(
                &terms[phrase.sequence[0]],
                &terms[phrase.sequence[1]],
            );
            bigram_probe[index] = Some(stats_fields.len());
            stats_fields.push(crate::pb::FieldTerms {
                field: column,
                terms: vec![bigram],
            });
        }
        Self {
            analyzed,
            stats_fields,
            bigram_probe,
            probe_from,
        }
    }

    pub(super) fn stats_fields(&self) -> &[crate::pb::FieldTerms] {
        &self.stats_fields
    }
    pub(super) fn probe_from(&self) -> usize {
        self.probe_from
    }

    pub(super) fn resolve(
        &self,
        fields: &[crate::pb::QueryField],
        globals: &FusedGlobals,
        shards: usize,
    ) -> Result<ResolvedFused, Status> {
        let mut resolved = ResolvedFused::new(globals, fields.len());
        for (index, field) in fields.iter().enumerate() {
            let item = self.resolve_field(index, field, globals, shards)?;
            resolved.push(item, globals);
        }
        Ok(resolved)
    }

    fn resolve_field(
        &self,
        index: usize,
        field: &crate::pb::QueryField,
        globals: &FusedGlobals,
        shards: usize,
    ) -> Result<ResolvedField, Status> {
        let fingerprint = crate::analyzer::analysis_fingerprint(field.analysis.as_ref());
        let analysis = &self.analyzed.fields[index];
        let phrase = analysis
            .phrase
            .as_ref()
            .filter(|phrase| phrase.sequence.len() >= 2 || !phrase.alternatives.is_empty());
        let Some(phrase) = phrase else {
            return Ok(ResolvedField {
                field: crate::pb::QueryField {
                    phrase: None,
                    ..field.clone()
                },
                terms: analysis.terms.clone(),
                source: index,
                phrase: None,
                fingerprint,
                routing: None,
            });
        };
        let bigram_everywhere =
            self.bigram_probe[index].is_some_and(|probe| globals.known_shards[probe] == shards);
        let positions_everywhere =
            globals.known_shards[index] == shards && globals.positions_shards[index] == shards;
        if !phrase.alternatives.is_empty() && !positions_everywhere {
            return Err(Status::invalid_argument(format!(
                "field {:?}: query lattice alternatives require token positions on every shard",
                field.field
            )));
        }
        let route = crate::proximity::choose_route(
            &field.field,
            phrase.sequence.len(),
            phrase.slop,
            bigram_everywhere,
            positions_everywhere,
        )
        .map_err(Status::invalid_argument)?;
        Ok(match route {
            crate::proximity::PhraseRoute::BigramColumn(column) => {
                let source = self.bigram_probe[index].expect("bigram route implies a probe");
                ResolvedField {
                    field: crate::pb::QueryField {
                        field: column.clone(),
                        phrase: None,
                        ..field.clone()
                    },
                    terms: self.stats_fields[source].terms.clone(),
                    source,
                    phrase: None,
                    fingerprint: crate::proximity::bigram_fingerprint(fingerprint),
                    routing: Some(crate::pb::PhraseRouting {
                        field: field.field.clone(),
                        served_field: column,
                        bigram_column: true,
                        slop: 0,
                    }),
                }
            }
            crate::proximity::PhraseRoute::Positions => {
                positions_field(index, field, analysis, phrase, fingerprint)
            }
        })
    }
}

/// A phrase leg verified against token positions on the field itself.
fn positions_field(
    index: usize,
    field: &crate::pb::QueryField,
    analysis: &AnalyzedField,
    phrase: &PhraseConstraint,
    fingerprint: u64,
) -> ResolvedField {
    ResolvedField {
        field: field.clone(),
        terms: analysis.terms.clone(),
        source: index,
        phrase: Some(crate::pb::PhraseLeg {
            sequence: phrase.sequence.iter().map(|&index| index as u32).collect(),
            slop: phrase.slop,
            alternatives: phrase
                .alternatives
                .iter()
                .map(|path| crate::pb::PhraseSequence {
                    terms: path.iter().map(|&index| index as u32).collect(),
                })
                .collect(),
        }),
        fingerprint,
        routing: Some(crate::pb::PhraseRouting {
            field: field.field.clone(),
            served_field: field.field.clone(),
            bigram_column: false,
            slop: phrase.slop,
        }),
    }
}

struct ResolvedField {
    field: crate::pb::QueryField,
    terms: Vec<String>,
    source: usize,
    phrase: Option<crate::pb::PhraseLeg>,
    fingerprint: u64,
    routing: Option<crate::pb::PhraseRouting>,
}
pub(super) struct ResolvedFused {
    pub(super) fields: Vec<crate::pb::QueryField>,
    pub(super) terms: Vec<Vec<String>>,
    pub(super) globals: FusedGlobals,
    pub(super) phrase_legs: Vec<Option<crate::pb::PhraseLeg>>,
    pub(super) fingerprints: Vec<u64>,
    pub(super) routing: Vec<crate::pb::PhraseRouting>,
}
impl ResolvedFused {
    fn new(globals: &FusedGlobals, capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
            terms: Vec::with_capacity(capacity),
            globals: FusedGlobals {
                doc_count: globals.doc_count,
                totals: Vec::with_capacity(capacity),
                dfs: Vec::with_capacity(capacity),
                epochs: globals.epochs.clone(),
                known_shards: Vec::with_capacity(capacity),
                positions_shards: Vec::with_capacity(capacity),
            },
            phrase_legs: Vec::with_capacity(capacity),
            fingerprints: Vec::with_capacity(capacity),
            routing: Vec::new(),
        }
    }
    fn push(&mut self, item: ResolvedField, globals: &FusedGlobals) {
        self.globals.totals.push(globals.totals[item.source]);
        self.globals.dfs.push(globals.dfs[item.source].clone());
        self.globals
            .known_shards
            .push(globals.known_shards[item.source]);
        self.globals
            .positions_shards
            .push(globals.positions_shards[item.source]);
        self.fields.push(item.field);
        self.terms.push(item.terms);
        self.phrase_legs.push(item.phrase);
        self.fingerprints.push(item.fingerprint);
        self.routing.extend(item.routing);
    }
}
