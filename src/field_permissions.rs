//! Field-use and disclosure checks for authority-issued search decisions.
use crate::{filter::LeafRef, pb::*, values::ValueLeaf};
use std::collections::BTreeMap;
use tonic::Status;

mod query;

#[derive(Clone, Debug)]
pub(crate) struct FieldScope {
    grants: BTreeMap<String, u8>,
    disclose_identity: bool,
    /// The index's derived columns (`docs/derived-columns.md`): a
    /// column declared `inputs` is usable or disclosable only when the
    /// same action is granted on every column it reads, and on the
    /// document identity when it reads the stable key. Hashing a field
    /// does not make it public.
    derived: Option<std::sync::Arc<crate::derived::Declaration>>,
}
impl FieldScope {
    /// Attach the declaration the grants are judged under.
    pub(crate) fn with_derived(
        mut self,
        derived: Option<std::sync::Arc<crate::derived::Declaration>>,
    ) -> Self {
        self.derived = derived;
        self
    }
    /// The inputs a derived column's grant depends on: `None` for a
    /// source column or a column declared `own`; otherwise its input
    /// columns and whether it reads the stable key.
    fn derived_inputs(&self, field: &str) -> Option<(&[String], bool)> {
        let column = self.derived.as_ref()?.column(field)?;
        match column.disclosure {
            crate::pb::DerivedDisclosure::Inputs => Some((&column.inputs, column.reads_stable_key)),
            _ => None,
        }
    }
    pub(crate) fn new(input: &FieldPermissions) -> Result<Self, String> {
        let mut grants = BTreeMap::new();
        for grant in &input.grants {
            if grant.field.is_empty() || grant.actions.is_empty() {
                return Err("field grants require a name and at least one action".into());
            }
            let mut bits = 0;
            for action in &grant.actions {
                let bit = match FieldAction::try_from(*action) {
                    Ok(FieldAction::Use) => 1,
                    Ok(FieldAction::Disclose) => 2,
                    _ => return Err("unknown field action".into()),
                };
                if bits & bit != 0 {
                    return Err("field grant repeats an action".into());
                }
                bits |= bit;
            }
            if grants.insert(grant.field.clone(), bits).is_some() {
                return Err("field permissions repeat a field grant".into());
            }
        }
        Ok(Self {
            grants,
            disclose_identity: input.disclose_document_identity,
            derived: None,
        })
    }
    pub(crate) fn can_disclose_identity(&self) -> bool {
        self.disclose_identity
    }
    fn granted(&self, field: &str, bit: u8) -> bool {
        if !self.grants.get(field).is_some_and(|bits| bits & bit != 0) {
            return false;
        }
        match self.derived_inputs(field) {
            None => true,
            Some((inputs, reads_stable_key)) => {
                inputs
                    .iter()
                    .all(|input| self.grants.get(input).is_some_and(|bits| bits & bit != 0))
                    && (!reads_stable_key || self.disclose_identity)
            }
        }
    }
    pub(crate) fn can_use(&self, field: &str) -> bool {
        self.granted(field, 1)
    }
    pub(crate) fn can_disclose(&self, field: &str) -> bool {
        self.granted(field, 2)
    }
    fn denied() -> Status {
        Status::permission_denied("field access is not granted")
    }
    fn require_use(&self, field: &str) -> Result<(), Status> {
        if self.can_use(field) {
            Ok(())
        } else {
            Err(Self::denied())
        }
    }
    fn require_disclose(&self, field: &str) -> Result<(), Status> {
        if self.can_disclose(field) {
            Ok(())
        } else {
            Err(Self::denied())
        }
    }
    pub(crate) fn dictionary(&self, field: &str) -> Result<(), Status> {
        self.require_use(field)?;
        self.require_disclose(field)
    }
    pub(crate) fn suggest(&self, req: &SuggestRequest) -> Result<(), Status> {
        let SuggestRequest {
            field,
            collection: _,
            prefix: _,
            limit: _,
            max_scan: _,
            analysis: _,
        } = req;
        self.dictionary(field)
    }
    pub(crate) fn term_suggest(&self, req: &TermSuggestRequest) -> Result<(), Status> {
        let TermSuggestRequest {
            field,
            collection: _,
            text: _,
            analysis: _,
            max_edits: _,
            prefix_length: _,
            limit: _,
            max_scan: _,
            mode: _,
        } = req;
        self.dictionary(field)
    }
    pub(crate) fn bm25(
        &self,
        req: &Bm25SearchRequest,
        user_filter: Option<&FilterExpr>,
        projections: &[CompiledProjection],
    ) -> Result<(), Status> {
        // Exhaustive bindings force new request fields through this audit.
        // Text/options are caller data; filter/projection IR was compiled once.
        let Bm25SearchRequest {
            text: _,
            k: _,
            analysis: _,
            min_score: _,
            fields: query_fields,
            facet_fields,
            score_stages,
            map_facet_fields,
            range_facet_fields,
            geo_filters,
            filter: _,
            stats_fields,
            cardinality_fields,
            projections: _,
            phrase: _,
            prefixes,
            highlight,
            collection: _,
            synonyms: _,
            synonyms_off: _,
            explain,
        } = req;
        let fields: Vec<&str> = if query_fields.is_empty() {
            vec!["body"]
        } else {
            query_fields.iter().map(|f| f.field.as_str()).collect()
        };
        for field in fields {
            self.require_use(field)?;
            if *explain {
                self.require_disclose(field)?;
            }
        }
        if !prefixes.is_empty() {
            self.dictionary("body")?;
        }
        for field in query_fields {
            if !field.prefixes.is_empty() {
                self.dictionary(&field.field)?;
            }
        }
        for field in facet_fields
            .iter()
            .chain(stats_fields)
            .chain(cardinality_fields)
        {
            self.dictionary(field)?;
        }
        for field in map_facet_fields {
            self.dictionary(&field.column)?;
        }
        for field in range_facet_fields {
            self.dictionary(&field.column)?;
        }
        for stage in score_stages {
            self.require_use(&stage.column)?;
            if *explain {
                self.require_disclose(&stage.column)?;
            }
        }
        self.filter(geo_filters, user_filter)?;
        self.fetch_values(projections, &[])?;
        if let Some(highlight) = highlight {
            if highlight.fields.is_empty() {
                self.dictionary("body")?;
            }
            for field in &highlight.fields {
                self.dictionary(field)?;
            }
        }
        Ok(())
    }
    pub(crate) fn browse(
        &self,
        filters: &crate::coordinator::RequestFilters,
        sort: &[BrowseSort],
        lexical_terms: &[String],
    ) -> Result<(), Status> {
        self.filter(&filters.geo, filters.tree.as_ref())?;
        if !lexical_terms.is_empty() {
            self.require_use("body")?;
        }
        for key in sort {
            self.dictionary(&key.column)?;
        }
        Ok(())
    }

    pub(crate) fn aggregate(
        &self,
        filters: &crate::coordinator::RequestFilters,
        compiled: &crate::coordinator::CompiledAggregate,
    ) -> Result<(), Status> {
        let crate::coordinator::CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            group_by,
            percentile_specs: _,
            max_groups: _,
        } = compiled;
        self.filter(&filters.geo, filters.tree.as_ref())?;
        if !group_by.is_empty() {
            self.dictionary(group_by)?;
        }
        for expr in aggregations
            .iter()
            .filter_map(|a| a.expr.as_ref())
            .chain(histograms.iter().filter_map(|h| h.expr.as_ref()))
            .chain(percentiles.iter().filter_map(|p| p.expr.as_ref()))
        {
            let mut leaves = Vec::new();
            crate::values::column_leaves(expr, &mut leaves);
            for leaf in leaves {
                let column = match leaf {
                    ValueLeaf::Column(column) | ValueLeaf::Map { column, .. } => column,
                };
                self.dictionary(&column)?;
            }
        }
        Ok(())
    }

    pub(crate) fn boolean_leaf(&self, leaf: &BooleanPlanLeaf) -> Result<(), Status> {
        match leaf.leaf.as_ref() {
            Some(boolean_plan_leaf::Leaf::Lexical(leaf)) => {
                self.require_use("body")?;
                for stage in &leaf.score_stages {
                    self.require_use(&stage.column)?;
                }
                Ok(())
            }
            Some(boolean_plan_leaf::Leaf::Dense(leaf)) => self.vector(&leaf.field),
            Some(boolean_plan_leaf::Leaf::Filter(leaf)) => {
                self.filter(&leaf.geo_filters, leaf.filter.as_ref())
            }
            None => Err(Status::invalid_argument("Boolean leaf has no kind")),
        }
    }

    pub(crate) fn vector(&self, field: &str) -> Result<(), Status> {
        self.require_use(field)
    }
    pub(crate) fn lexical_scores(&self, stages: &[ScoreStage]) -> Result<(), Status> {
        self.require_use("body")?;
        for stage in stages {
            self.require_use(&stage.column)?;
        }
        Ok(())
    }
    pub(crate) fn lexical_membership(&self) -> Result<(), Status> {
        self.require_use("body")
    }
    pub(crate) fn filter(
        &self,
        geo_filters: &[GeoFilter],
        user_filter: Option<&FilterExpr>,
    ) -> Result<(), Status> {
        for geo in geo_filters {
            self.require_use(&geo.column)?;
        }
        if let Some(filter) = user_filter {
            let mut allowed = true;
            crate::filter::walk_leaves(filter, &mut |leaf| {
                let column = match leaf {
                    LeafRef::Facet(p) => &p.column,
                    LeafRef::Number(p) => &p.column,
                    LeafRef::MapFacet(p) => &p.column,
                    LeafRef::MapNumber(p) => &p.column,
                    LeafRef::MapHasKey(p) => &p.column,
                    LeafRef::Has(p) => &p.column,
                    LeafRef::Geo(p) => &p.column,
                    LeafRef::StringRange(p) | LeafRef::MapStringRange(p) => &p.column,
                    LeafRef::StringPrefix(p) | LeafRef::MapStringPrefix(p) => &p.column,
                };
                allowed &= self.can_use(column);
            });
            if !allowed {
                return Err(Self::denied());
            }
        }
        Ok(())
    }

    /// Stored-value dimensions use their inputs internally; projected values
    /// disclose them. Explanation disclosure is checked by the query planner.
    pub(crate) fn fetch_values(
        &self,
        projections: &[CompiledProjection],
        stages: &[ScoreStage],
    ) -> Result<(), Status> {
        for stage in stages {
            self.require_use(&stage.column)?;
        }
        for projection in projections {
            let mut leaves = Vec::new();
            if let Some(expr) = &projection.expr {
                crate::values::column_leaves(expr, &mut leaves);
            }
            for leaf in leaves {
                let column = match leaf {
                    ValueLeaf::Column(column) | ValueLeaf::Map { column, .. } => column,
                };
                self.dictionary(&column)?;
            }
        }
        Ok(())
    }
    /// Explicit detail requests require permission; automatic details may be
    /// omitted with a visible redaction flag while preserving the ranking.
    pub(crate) fn disclose(&self, response: &mut Bm25SearchResponse) -> Result<(), Status> {
        let mut redacted = false;
        for hit in &mut response.hits {
            let Bm25Hit {
                doc_id: _,
                score: _,
                terms,
                projected: _,
                snippets,
                explain,
                identity,
            } = hit;
            terms.retain(|term| {
                let field = if term.field.is_empty() {
                    "body"
                } else {
                    &term.field
                };
                let keep = self.can_disclose(field);
                redacted |= !keep;
                keep
            });
            for snippet in snippets {
                self.require_disclose(&snippet.field)?;
            }
            if let Some(explain) = explain {
                for term in &explain.terms {
                    self.require_disclose(&term.field)?;
                }
                for stage in &explain.stages {
                    self.require_disclose(&stage.column)?;
                }
            }
            if !self.disclose_identity && identity.take().is_some() {
                redacted = true;
            }
        }
        response.synonym_expansions.retain(|expansion| {
            let keep = self.can_disclose(&expansion.field);
            redacted |= !keep;
            keep
        });
        response.phrase_routing.retain(|route| {
            let keep = self.can_disclose(&route.field) && self.can_disclose(&route.served_field);
            redacted |= !keep;
            keep
        });
        response.field_details_redacted = redacted;
        Ok(())
    }
}

#[cfg(test)]
mod derived_tests {
    use super::*;

    fn scope(grants: &[(&str, &[FieldAction])], identity: bool) -> FieldScope {
        let permissions = FieldPermissions {
            grants: grants
                .iter()
                .map(|(field, actions)| FieldGrant {
                    field: field.to_string(),
                    actions: actions.iter().map(|a| *a as i32).collect(),
                })
                .collect(),
            disclose_document_identity: identity,
        };
        let declaration = crate::derived::Declaration::compile(&DerivedColumns {
            columns: vec![
                DerivedColumn {
                    name: "court_hash".into(),
                    expression: "hash.fnv64(court)".into(),
                    kind: MaterializeKind::U64 as i32,
                    disclosure: DerivedDisclosure::Inputs as i32,
                },
                DerivedColumn {
                    name: "key_bucket".into(),
                    expression: "hash.fnv64(stable_key()) % 64u".into(),
                    kind: MaterializeKind::U64 as i32,
                    disclosure: DerivedDisclosure::Inputs as i32,
                },
                DerivedColumn {
                    name: "decade".into(),
                    expression: "year / 10".into(),
                    kind: MaterializeKind::I64 as i32,
                    disclosure: DerivedDisclosure::Own as i32,
                },
            ],
        })
        .unwrap();
        FieldScope::new(&permissions)
            .unwrap()
            .with_derived(Some(std::sync::Arc::new(declaration)))
    }

    #[test]
    fn a_derived_column_needs_the_grants_of_its_inputs() {
        use FieldAction::{Disclose, Use};
        // The column's own grant is not enough: hashing court does not
        // make court public.
        let s = scope(&[("court_hash", &[Use, Disclose])], false);
        assert!(!s.can_use("court_hash"));
        assert!(!s.can_disclose("court_hash"));
        let s = scope(
            &[("court_hash", &[Use, Disclose]), ("court", &[Use])],
            false,
        );
        assert!(s.can_use("court_hash"));
        assert!(
            !s.can_disclose("court_hash"),
            "disclosing needs disclose on the input"
        );
        let s = scope(
            &[
                ("court_hash", &[Use, Disclose]),
                ("court", &[Use, Disclose]),
            ],
            false,
        );
        assert!(s.can_disclose("court_hash"));
        // An input grant without the column's own grant is not enough either.
        let s = scope(&[("court", &[Use, Disclose])], false);
        assert!(!s.can_use("court_hash"));
        // The stable key is the document identity.
        let s = scope(&[("key_bucket", &[Use, Disclose])], false);
        assert!(!s.can_use("key_bucket"));
        let s = scope(&[("key_bucket", &[Use, Disclose])], true);
        assert!(s.can_use("key_bucket") && s.can_disclose("key_bucket"));
        // A column declared `own` is judged on its own grant.
        let s = scope(&[("decade", &[Use])], false);
        assert!(s.can_use("decade") && !s.can_disclose("decade"));
        // Source columns are unchanged.
        let s = scope(&[("year", &[Use, Disclose])], false);
        assert!(s.can_use("year") && s.can_disclose("year") && !s.can_use("decade"));
    }
}
