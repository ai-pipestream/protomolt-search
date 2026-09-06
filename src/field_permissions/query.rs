//! Query-wide field admission and automatic-detail disclosure. This runs
//! before shard reads; per-route checks remain at the internal boundaries.
use super::FieldScope;
use crate::pb::*;
use std::collections::BTreeSet;
use tonic::Status;

pub(crate) struct QueryDisclosure {
    fields: FieldScope,
    hidden_dimensions: BTreeSet<String>,
}

impl FieldScope {
    pub(crate) fn query(&self, req: &QueryRequest) -> Result<QueryDisclosure, Status> {
        let QueryRequest {
            request_id: _,
            k: _,
            selection_k: _,
            selection,
            boosts,
            scorer,
            profile: _,
            cursor: _,
            sort,
            projections,
            required_topology_generation: _,
            highlight,
            collection: _,
            collapse,
            explain,
            aggregate,
        } = req;
        self.selection(
            selection
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("a query needs a selection tree"))?,
            *explain,
            0,
        )?;
        for BoostQuery {
            query,
            window: _,
            base_weight: _,
            boost_weight: _,
        } in boosts
        {
            self.search_leaf(
                query
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("boost has no query"))?,
                *explain,
            )?;
        }
        for QuerySort {
            column,
            descending: _,
        } in sort
        {
            self.dictionary(column)?;
        }
        if let Some(CollapseSpec {
            column,
            inner_hits: _,
        }) = collapse
        {
            self.dictionary(column)?;
        }
        self.fetch_values(&crate::coordinator::compile_projections(projections)?, &[])?;
        if let Some(spec) = highlight {
            if spec.fields.is_empty() {
                self.dictionary("body")?;
            }
            for field in &spec.fields {
                self.dictionary(field)?;
            }
        }
        if let Some(aggregate) = aggregate {
            self.query_aggregate(aggregate)?;
        }
        let mut hidden_dimensions = BTreeSet::new();
        if let Some(CompositeScorer {
            operation: _,
            dimensions,
        }) = scorer
        {
            for ScoreDimension {
                id,
                weight: _,
                source,
                normalization: _,
                missing: _,
            } in dimensions
            {
                if let Some(score_signal::Source::BoundedValue(stage)) =
                    source.as_ref().and_then(|s| s.source.as_ref())
                {
                    self.require_use(&stage.column)?;
                    if *explain {
                        self.require_disclose(&stage.column)?;
                    }
                    if !self.can_disclose(&stage.column) {
                        hidden_dimensions.insert(id.clone());
                    }
                }
            }
        }
        Ok(QueryDisclosure {
            fields: self.clone(),
            hidden_dimensions,
        })
    }

    fn selection(&self, node: &SelectionQuery, explain: bool, depth: usize) -> Result<(), Status> {
        if depth > 64 {
            return Err(Status::invalid_argument("selection exceeds 64 levels"));
        }
        match node.node.as_ref() {
            Some(selection_query::Node::Search(search)) => self.search_leaf(search, explain),
            Some(selection_query::Node::Filter(FilterQuery { id: _, predicate })) => {
                match predicate.as_ref() {
                    Some(filter_query::Predicate::Cel(cel)) => {
                        self.filter(&[], crate::cel::compile_filter(cel)?.as_ref())
                    }
                    Some(filter_query::Predicate::Geo(geo)) => {
                        self.filter(std::slice::from_ref(geo), None)
                    }
                    None => Err(Status::invalid_argument("filter has no predicate")),
                }
            }
            Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: _,
                clauses,
                scoring: _,
            })) => {
                for child in clauses {
                    self.selection(child, explain, depth + 1)?;
                }
                Ok(())
            }
            Some(selection_query::Node::Boolean(BooleanQuery {
                must,
                should,
                must_not,
                minimum_should_match: _,
                aggregate,
            })) => {
                for child in must.iter().chain(should).chain(must_not) {
                    self.selection(child, explain, depth + 1)?;
                }
                if let Some(aggregate) = aggregate {
                    self.query_aggregate(aggregate)?;
                }
                Ok(())
            }
            None => Err(Status::invalid_argument("selection has no node")),
        }
    }

    fn search_leaf(
        &self,
        SearchQuery { id: _, query }: &SearchQuery,
        explain: bool,
    ) -> Result<(), Status> {
        match query.as_ref() {
            Some(search_query::Query::Lexical(LexicalQuery {
                text: _,
                analysis: _,
                score_stages,
                phrase: _,
                prefixes,
                synonyms: _,
                synonyms_off: _,
            })) => {
                self.require_use("body")?;
                if explain || !prefixes.is_empty() {
                    self.require_disclose("body")?;
                }
                for stage in score_stages {
                    self.require_use(&stage.column)?;
                    if explain {
                        self.require_disclose(&stage.column)?;
                    }
                }
                Ok(())
            }
            Some(search_query::Query::Dense(DenseQuery {
                vector: _,
                score_mode: _,
                quality: _,
                execution_mode: _,
                field,
            })) => {
                self.vector(field)?;
                if explain {
                    self.require_disclose(field)?;
                }
                Ok(())
            }
            None => Err(Status::invalid_argument("search leaf has no query")),
        }
    }

    fn query_aggregate(&self, req: &AggregateRequest) -> Result<(), Status> {
        let filters = crate::coordinator::RequestFilters::compile(&req.geo_filters, &req.filter)?;
        self.aggregate(&filters, &crate::coordinator::compile_aggregations(req)?)
    }
}

impl QueryDisclosure {
    pub(crate) fn apply(&self, response: &mut QueryResponse) {
        // Redaction is explicit even when an internal lexical adapter already
        // removed identity or dictionary details from its intermediate answer.
        let mut redacted = !self.fields.disclose_identity || !self.hidden_dimensions.is_empty();
        for hit in response.hits.iter_mut().chain(
            response
                .groups
                .iter_mut()
                .flat_map(|group| group.hits.iter_mut()),
        ) {
            if !self.fields.disclose_identity {
                hit.identity = None;
            }
            hit.dimensions
                .retain(|dimension| !self.hidden_dimensions.contains(&dimension.id));
        }
        response.synonym_expansions.retain(|expansion| {
            let keep = self.fields.can_disclose(&expansion.field);
            redacted |= !keep;
            keep
        });
        response.field_details_redacted |= redacted;
    }
}
