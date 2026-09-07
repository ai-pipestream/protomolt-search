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
            self.dictionary(crate::sortkeys::target_field(
                &key.column,
                key.map.as_ref(),
            )?)?;
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
                    ValueLeaf::Column(column)
                    | ValueLeaf::Map { column, .. }
                    | ValueLeaf::TypedMap { column, .. } => column,
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
                    LeafRef::MapNumber(p) | LeafRef::TypedMapNumber(p) => &p.column,
                    LeafRef::MapHasKey(p) | LeafRef::TypedMapHasKey(p) => &p.column,
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
                    ValueLeaf::Column(column)
                    | ValueLeaf::Map { column, .. }
                    | ValueLeaf::TypedMap { column, .. } => column,
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

    /// A lexical selection tree for the Query route's checks.
    fn selection() -> SelectionQuery {
        SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: String::new(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "alpha".into(),
                    ..Default::default()
                })),
            })),
        }
    }

    fn query_request(column: &str) -> QueryRequest {
        QueryRequest {
            selection: Some(selection()),
            sort: vec![QuerySort {
                column: column.into(),
                descending: false,
                map: None,
            }],
            projections: vec![NamedProjection {
                name: "p".into(),
                expression: column.into(),
            }],
            ..Default::default()
        }
    }

    fn bm25_request(column: &str, explain: bool) -> Bm25SearchRequest {
        Bm25SearchRequest {
            text: "alpha".into(),
            facet_fields: vec![column.into()],
            score_stages: vec![ScoreStage {
                column: column.into(),
                ..Default::default()
            }],
            projections: vec![NamedProjection {
                name: "p".into(),
                expression: column.into(),
            }],
            explain,
            ..Default::default()
        }
    }

    fn aggregate_request(column: &str) -> AggregateRequest {
        AggregateRequest {
            aggregations: vec![Aggregation {
                name: "total".into(),
                expression: column.into(),
                op: AggregateOp::Sum as i32,
                max_distinct: 0,
            }],
            ..Default::default()
        }
    }

    /// A refused read is PermissionDenied, whatever the route.
    fn denied<T>(r: Result<T, Status>) {
        match r {
            Ok(_) => panic!("the read must refuse"),
            Err(e) => assert_eq!(e.code(), tonic::Code::PermissionDenied),
        }
    }

    #[test]
    fn every_query_path_judges_a_derived_column_on_its_inputs() {
        use FieldAction::{Disclose, Use};
        // body is the lexical field every request reads; the variants
        // differ only in the derived column's grants.
        let body: (&str, &[FieldAction]) = ("body", &[Use, Disclose]);
        let both = || {
            scope(
                &[
                    body,
                    ("court_hash", &[Use, Disclose]),
                    ("court", &[Use, Disclose]),
                ],
                false,
            )
        };
        let no_input = || scope(&[body, ("court_hash", &[Use, Disclose])], false);
        let use_only_input = || {
            scope(
                &[body, ("court_hash", &[Use, Disclose]), ("court", &[Use])],
                false,
            )
        };

        // Filters.
        let filter = crate::cel::compile_filter("court_hash == 42u").unwrap();
        assert!(both().filter(&[], filter.as_ref()).is_ok());
        denied(no_input().filter(&[], filter.as_ref()));
        assert!(use_only_input().filter(&[], filter.as_ref()).is_ok());

        // Facets, score stages and projections on the BM25 route.
        let req = bm25_request("court_hash", false);
        let projections = crate::coordinator::compile_projections(&req.projections).unwrap();
        assert!(both().bm25(&req, None, &projections).is_ok());
        denied(no_input().bm25(&req, None, &projections));
        // The input's Use grant admits the filter and the scoring read,
        // but facets and projections disclose, so they still refuse.
        denied(use_only_input().bm25(&req, None, &projections));
        let stage_only = Bm25SearchRequest {
            text: "alpha".into(),
            score_stages: req.score_stages.clone(),
            ..Default::default()
        };
        assert!(use_only_input().bm25(&stage_only, None, &[]).is_ok());

        // Explanations disclose the column.
        let req = bm25_request("court_hash", true);
        let projections = crate::coordinator::compile_projections(&req.projections).unwrap();
        denied(no_input().bm25(&req, None, &projections));
        denied(use_only_input().bm25(&req, None, &projections));

        // Sorts and projections on the Query route.
        let req = query_request("court_hash");
        assert!(both().query(&req).is_ok());
        denied(no_input().query(&req).map(|_| ()));
        denied(use_only_input().query(&req).map(|_| ()));

        // Aggregations read the column's values, dictionary-style.
        let req = aggregate_request("court_hash");
        let compiled = crate::coordinator::compile_aggregations(&req).unwrap();
        let filters = crate::coordinator::RequestFilters::default();
        assert!(both().aggregate(&filters, &compiled).is_ok());
        denied(no_input().aggregate(&filters, &compiled));
        denied(use_only_input().aggregate(&filters, &compiled));

        // Browse sorts disclose.
        let sort = vec![BrowseSort {
            column: "court_hash".into(),
            descending: false,
            map: None,
        }];
        assert!(both().browse(&filters, &sort, &[]).is_ok());
        denied(no_input().browse(&filters, &sort, &[]));

        // The disclosure pass on a response: an explain stage naming
        // the derived column discloses it.
        let response = |column: &str| Bm25SearchResponse {
            hits: vec![Bm25Hit {
                doc_id: 1,
                score: 1.0,
                explain: Some(Bm25Explain {
                    stages: vec![ScoreStageExplain {
                        column: column.into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut r = response("court_hash");
        assert!(both().disclose(&mut r).is_ok());
        denied(no_input().disclose(&mut response("court_hash")));
        denied(use_only_input().disclose(&mut response("court_hash")));
    }

    #[test]
    fn the_stable_key_column_needs_the_identity_grant_and_own_needs_its_own() {
        use FieldAction::{Disclose, Use};
        let body: (&str, &[FieldAction]) = ("body", &[Use, Disclose]);
        // key_bucket reads stable_key(): the column's grants plus the
        // document-identity grant, on every path.
        let req = query_request("key_bucket");
        denied(scope(&[body, ("key_bucket", &[Use, Disclose])], false).query(&req));
        assert!(scope(&[body, ("key_bucket", &[Use, Disclose])], true)
            .query(&req)
            .is_ok());
        // A column declared `own` is judged on its own grant: the
        // input's grant neither helps nor is required.
        let req = query_request("decade");
        denied(scope(&[body, ("year", &[Use, Disclose])], false).query(&req));
        assert!(scope(&[body, ("decade", &[Use, Disclose])], false)
            .query(&req)
            .is_ok());
        // The aggregate route judges `own` the same way.
        let req = aggregate_request("decade");
        let compiled = crate::coordinator::compile_aggregations(&req).unwrap();
        let filters = crate::coordinator::RequestFilters::default();
        denied(scope(&[body, ("year", &[Use, Disclose])], false).aggregate(&filters, &compiled));
        assert!(scope(&[body, ("decade", &[Use, Disclose])], false)
            .aggregate(&filters, &compiled)
            .is_ok());
    }
}
