//! Validated flat and fused lexical request boundaries.

use super::*;
use std::borrow::Cow;

/// A validated single-field lexical search with its projections.
#[derive(Clone, Copy)]
pub struct FlatLexicalQuery<'a> {
    options: ValidatedLexicalQuery<'a>,
    projections: &'a [crate::pb::CompiledProjection],
}

/// A validated multi-field lexical search.
pub struct FusedLexicalQuery<'a> {
    options: ValidatedLexicalQuery<'a>,
    fields: Cow<'a, [crate::pb::QueryField]>,
}

#[derive(Clone, Copy)]
pub(super) struct ValidatedLexicalQuery<'a> {
    input: &'a Bm25SearchRequest,
    k: u32,
    filter: Option<&'a crate::pb::FilterExpr>,
    highlight: bool,
}

impl<'a> FlatLexicalQuery<'a> {
    pub fn new(
        request: &'a Bm25SearchRequest,
        k: u32,
        projections: &'a [crate::pb::CompiledProjection],
        filter: Option<&'a crate::pb::FilterExpr>,
    ) -> Result<Self, Status> {
        if !request.fields.is_empty() || request.phrase.is_some() {
            return Err(Status::invalid_argument(
                "flat lexical context cannot carry query fields or a phrase; use the fused context",
            ));
        }
        Ok(Self {
            options: ValidatedLexicalQuery::new(request, k, filter)?,
            projections,
        })
    }

    pub(super) fn input(self) -> &'a Bm25SearchRequest {
        self.options.input
    }

    pub(super) fn options(self) -> ValidatedLexicalQuery<'a> {
        self.options
    }

    pub(super) fn projections(self) -> &'a [crate::pb::CompiledProjection] {
        self.projections
    }

    pub(super) fn with_k(self, k: u32) -> Self {
        Self {
            options: self.options.with_k(k),
            ..self
        }
    }
}

impl<'a> FusedLexicalQuery<'a> {
    pub fn new(
        request: &'a Bm25SearchRequest,
        k: u32,
        filter: Option<&'a crate::pb::FilterExpr>,
    ) -> Result<Self, Status> {
        validate_fused_outputs(request)?;
        if request.analysis.is_some() || request.phrase.is_some() || !request.prefixes.is_empty() {
            return Err(Status::invalid_argument("fused lexical context cannot carry flat-only analysis, terms, phrase, or expansions; put analysis and expansions on each query field"));
        }
        let options = ValidatedLexicalQuery::new(request, k, filter)?;
        validate_fields(&request.fields)?;
        Ok(Self {
            options,
            fields: Cow::Borrowed(&request.fields),
        })
    }

    pub(crate) fn body_phrase(
        request: &'a Bm25SearchRequest,
        k: u32,
        filter: Option<&'a crate::pb::FilterExpr>,
    ) -> Result<Self, Status> {
        validate_fused_outputs(request)?;
        if !request.fields.is_empty() || request.phrase.is_none() {
            return Err(Status::invalid_argument(
                "body phrase context requires a flat phrase request",
            ));
        }
        let options = ValidatedLexicalQuery::new(request, k, filter)?;
        let field = crate::pb::QueryField {
            field: "body".into(),
            analysis: request.analysis.clone(),
            weight: 1.0,
            phrase: request.phrase,
            prefixes: request.prefixes.clone(),
            synonyms: request.synonyms.clone(),
            synonyms_off: request.synonyms_off,
            ..Default::default()
        };
        Ok(Self {
            options,
            fields: Cow::Owned(vec![field]),
        })
    }

    pub(super) fn input(&self) -> &'a Bm25SearchRequest {
        self.options.input
    }

    pub(super) fn options(&self) -> ValidatedLexicalQuery<'a> {
        self.options
    }

    pub(super) fn fields(&self) -> &[crate::pb::QueryField] {
        &self.fields
    }
}

fn validate_fused_outputs(request: &Bm25SearchRequest) -> Result<(), Status> {
    if !request.score_stages.is_empty()
        || !request.projections.is_empty()
        || !request.stats_fields.is_empty()
        || !request.cardinality_fields.is_empty()
    {
        return Err(Status::invalid_argument(
            "fused lexical context cannot carry score stages, projections, or aggregations",
        ));
    }
    Ok(())
}

fn validate_fields(fields: &[crate::pb::QueryField]) -> Result<(), Status> {
    let mut seen = Vec::new();
    for field in fields {
        if field.field.is_empty() {
            return Err(Status::invalid_argument("QueryField.field must be named"));
        }
        if seen.contains(&field.field.as_str()) {
            return Err(Status::invalid_argument(format!(
                "field {:?} repeats in the query",
                field.field
            )));
        }
        if field.weight < 0.0 || field.weight.is_nan() {
            return Err(Status::invalid_argument(format!(
                "field {:?}: weight must be >= 0",
                field.field
            )));
        }
        seen.push(field.field.as_str());
    }
    Ok(())
}

impl<'a> ValidatedLexicalQuery<'a> {
    pub(super) fn new(
        request: &'a Bm25SearchRequest,
        k: u32,
        filter: Option<&'a crate::pb::FilterExpr>,
    ) -> Result<Self, Status> {
        if let Some(spec) = request.highlight.as_ref() {
            crate::highlight::Plan::from_spec(spec)?;
        }
        if request.min_score.is_nan() || request.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        crate::node::validate_range_facet_fields(&request.range_facet_fields)?;
        crate::node::validate_geo_filters(&request.geo_filters)?;
        if let Some(filter) = filter {
            crate::filter::validate_filter(filter)?;
        }
        Ok(Self {
            input: request,
            k,
            filter,
            highlight: true,
        })
    }

    pub(super) fn request(self) -> Bm25QueryRequest {
        Bm25QueryRequest {
            k: self.k,
            min_score: self.input.min_score,
            facet_fields: self.input.facet_fields.clone(),
            map_facet_fields: self.input.map_facet_fields.clone(),
            range_facet_fields: self.input.range_facet_fields.clone(),
            geo_filters: self.input.geo_filters.clone(),
            filter: self.filter.cloned(),
            highlight: self
                .highlight
                .then(|| self.input.highlight.clone())
                .flatten(),
            explain: self.input.explain,
            ..Default::default()
        }
    }

    pub(super) fn k(self) -> u32 {
        self.k
    }

    pub(super) fn with_k(self, k: u32) -> Self {
        Self { k, ..self }
    }

    pub(super) fn without_highlight(self) -> Self {
        Self {
            highlight: false,
            ..self
        }
    }
}

pub(super) struct LexicalExpansions<'a> {
    pub(super) prefixes: &'a mut Vec<crate::pb::PrefixExpansion>,
    pub(super) synonyms: &'a mut Vec<crate::pb::SynonymExpansion>,
}

pub(super) struct FlatRound<'a> {
    pub query: FlatLexicalQuery<'a>,
    pub terms: &'a [String],
    pub analysis_fingerprint: u64,
    pub global: &'a CorpusStats,
    pub claims: &'a [StatsClaim],
}

pub(super) struct FusedRound<'a> {
    pub query: ValidatedLexicalQuery<'a>,
    pub fields: &'a [crate::pb::QueryField],
    pub field_terms: &'a [Vec<String>],
    pub globals: &'a FusedGlobals,
    pub claims: &'a [StatsClaim],
    pub phrase: Option<(usize, &'a [f32], u64)>,
    pub phrase_legs: &'a [Option<crate::pb::PhraseLeg>],
    pub fingerprints: &'a [u64],
    pub timing: RoundTiming,
}

#[derive(Clone, Copy)]
pub(super) struct RoundTiming {
    pub trace: bool,
    pub started: std::time::Instant,
    pub analyzed: std::time::Duration,
    pub stats: std::time::Duration,
}
