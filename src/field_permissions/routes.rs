//! Per-route field admission: what each internal read path must be
//! granted before it touches a column.
use super::FieldScope;
use crate::{filter::LeafRef, pb::*, values::ValueLeaf};
use tonic::Status;

impl FieldScope {
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
            pre_analyzed_terms: _,
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
            self.highlight(highlight)?;
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
                    LeafRef::StringRange(p) => &p.column,
                    LeafRef::StringPrefix(p) => &p.column,
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

    /// Highlighting reads and discloses each field it marks; a spec that
    /// names none marks `body`.
    pub(super) fn highlight(&self, spec: &HighlightSpec) -> Result<(), Status> {
        if spec.fields.is_empty() {
            self.dictionary("body")?;
        }
        for field in &spec.fields {
            self.dictionary(field)?;
        }
        Ok(())
    }
}
