//! Lexical request inputs and validated round contexts.

use super::*;

#[derive(Clone, Copy, Default)]
pub struct LexicalFanout<'a> {
    pub text: &'a str,
    pub pre_analyzed_terms: &'a [String],
    pub k: u32,
    pub spec: Option<&'a crate::pb::AnalysisSpec>,
    pub fields: &'a [crate::pb::QueryField],
    pub min_score: f32,
    pub facet_fields: &'a [String],
    pub map_facet_fields: &'a [crate::pb::MapFacetField],
    pub range_facet_fields: &'a [crate::pb::RangeFacetField],
    pub score_stages: &'a [crate::pb::ScoreStage],
    pub geo_filters: &'a [crate::pb::GeoFilter],
    pub filter: Option<&'a crate::pb::FilterExpr>,
    pub stats_fields: &'a [String],
    pub cardinality_fields: &'a [String],
    pub projections: &'a [crate::pb::CompiledProjection],
    pub prefixes: &'a [crate::pb::TermPrefix],
    pub highlight: Option<&'a crate::pb::HighlightSpec>,
    pub synonyms: &'a [crate::pb::SynonymRule],
    pub synonyms_off: bool,
    pub explain: bool,
}

pub(super) struct LexicalExpansions<'a> {
    pub(super) prefixes: &'a mut Vec<crate::pb::PrefixExpansion>,
    pub(super) synonyms: &'a mut Vec<crate::pb::SynonymExpansion>,
}

#[derive(Clone, Copy)]
pub(super) struct ValidatedLexicalQuery<'a>(LexicalFanout<'a>);

impl<'a> LexicalFanout<'a> {
    pub(super) fn validate(self) -> Result<ValidatedLexicalQuery<'a>, Status> {
        crate::node::validate_range_facet_fields(self.range_facet_fields)?;
        crate::node::validate_geo_filters(self.geo_filters)?;
        if let Some(filter) = self.filter {
            crate::filter::validate_filter(filter)?;
        }
        Ok(ValidatedLexicalQuery(self))
    }
}

impl<'a> ValidatedLexicalQuery<'a> {
    pub(super) fn request(self) -> Bm25QueryRequest {
        let input = self.0;
        Bm25QueryRequest {
            k: input.k,
            min_score: input.min_score,
            facet_fields: input.facet_fields.to_vec(),
            map_facet_fields: input.map_facet_fields.to_vec(),
            range_facet_fields: input.range_facet_fields.to_vec(),
            geo_filters: input.geo_filters.to_vec(),
            filter: input.filter.cloned(),
            highlight: input.highlight.cloned(),
            explain: input.explain,
            ..Default::default()
        }
    }

    pub(super) fn input(self) -> LexicalFanout<'a> {
        self.0
    }

    pub(super) fn with_k(self, k: u32) -> Self {
        Self(LexicalFanout { k, ..self.0 })
    }
}

pub(super) struct FlatRound<'a> {
    pub query: ValidatedLexicalQuery<'a>,
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
