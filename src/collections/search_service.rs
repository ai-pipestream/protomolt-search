//! The `SearchService` impl over a [`CollectionSet`], generated so the
//! `async_trait` attribute sees every method.

use super::*;

/// The `SearchService` impl, produced by one macro so the `async_trait`
/// attribute sees every method: each unary request goes to the member
/// its `collection` names, with the resolved name written on it so the
/// member's own admission check sees the same answer.
macro_rules! search_service_over_collections {
    ($( $name:ident [$action:ident] : $req:ty => $resp:ty ),* $(,)?) => {
        #[tonic::async_trait]
        impl SearchService for CollectionSet {
            type QueryStreamStream =
                Guarded<DisclosedQueryStream<AuthorizedStream<<CoordinatorServiceImpl as SearchService>::QueryStreamStream>>>;

            $(
                async fn $name(
                    &self,
                    mut request: Request<$req>,
                ) -> Result<Response<$resp>, Status> {
                    let started = std::time::Instant::now();
                    let principal = self.authenticate(&request)?;
                    let (name, target, access) = self.resolve_authorized(principal.as_ref(), &request.get_ref().collection, AccessAction::$action)?;
                    let error_scope = ErrorScope::new(access.as_ref().map(|p| p.decision()));
                    let target = target.for_access(access.as_ref().map(|p| p.decision()), stringify!($name))
                        .map_err(|error| error_scope.status(error))?;
                    let _permit =
                        Self::admit_under(principal.as_ref(), &request, target.max_k())
                            .map_err(|error| error_scope.status(error))?;
                    request.get_mut().collection = name.clone();
                    let k = request.get_ref().k().unwrap_or(0);
                    let who = principal.as_ref().map(|p| p.name.clone()).unwrap_or_default();
                    if let Some(access) = &access { request.extensions_mut().insert(access.decision().clone()); }
                    let out = SearchService::$name(&target, request).await;
                    let out = match &access { Some(access) => access.check().and(out), None => out };
                    self.record(
                        stringify!($name),
                        &name,
                        k,
                        who,
                        started,
                        out.as_ref()
                            .map(|r| RecentFields::figures(r.get_ref()))
                            .map_err(Status::code),
                    );
                    out.map_err(|error| error_scope.status(error))
                }
            )*

            async fn query_stream(
                &self,
                mut request: Request<QueryStreamRequest>,
            ) -> Result<Response<Self::QueryStreamStream>, Status> {
                let started = std::time::Instant::now();
                let principal = self.authenticate(&request)?;
                let (name, target, access) = self.resolve_authorized(principal.as_ref(), &request.get_ref().collection, AccessAction::Search)?;
                let error_scope = ErrorScope::new(access.as_ref().map(|p| p.decision()));
                let target = target.for_access(access.as_ref().map(|p| p.decision()), "query_stream")
                    .map_err(|error| error_scope.status(error))?;
                let permit = Self::admit_under(principal.as_ref(), &request, target.max_k())
                    .map_err(|error| error_scope.status(error))?;
                request.get_mut().collection = name.clone();
                let k = request.get_ref().k().unwrap_or(0);
                let who = principal.as_ref().map(|p| p.name.clone()).unwrap_or_default();
                if let Some(access) = &access { request.extensions_mut().insert(access.decision().clone()); }
                let response = SearchService::query_stream(&target, request).await;
                let response = match &access { Some(access) => access.check().and(response), None => response };
                self.record(
                    "query_stream",
                    &name,
                    k,
                    who,
                    started,
                    response
                        .as_ref()
                        .map(|_| crate::diagnostics::RecentFigures::default())
                        .map_err(Status::code),
                );
                let response = response.map_err(|error| error_scope.status(error))?;
                Ok(response.map(|stream| Guarded::new(DisclosedQueryStream::new(
                    AuthorizedStream::new(stream, access), error_scope), permit)))
            }

            async fn routed_ingest_mapped(
                &self,
                request: Request<Streaming<RoutedIngestMappedRequest>>,
            ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
                let (_permit, principal) = self.admit(&request)?;
                let mut inbound = MeteredIngest::new(request.into_inner(), principal.clone());
                let mut bind = CoordinatorServiceImpl::routed_bind(&mut inbound).await?;
                let (name, target, access) = self.resolve_authorized(principal.as_ref(), &bind.collection, AccessAction::Ingest)?;
                let target = target.clone();
                bind.collection = name;
                let inbound = AuthorizedStream::new(inbound, access.clone());
                let response = target.routed_ingest_mapped_bound(bind, inbound).await;
                if let Some(access) = &access { access.check()?; }
                response.map(Response::new)
            }

            async fn cluster_health(
                &self,
                mut request: Request<ClusterHealthRequest>,
            ) -> Result<Response<ClusterHealthResponse>, Status> {
                let (_permit, principal) = self.admit(&request)?;
                let requested = request.get_ref().collection.clone();
                // An unnamed health request on a named set lists every
                // collection separately; counts are never summed across them.
                if requested.is_empty() && self.members.unnamed.is_none() {
                    let mut collections = Vec::with_capacity(self.members.named.len());
                    let mut decisions = Vec::new();
                    for (name, member) in &self.members.named {
                        let access = match self.authorize(principal.as_ref(), name, AccessAction::Admin) {
                            Ok(access) => access,
                            Err(error) if error.code() == tonic::Code::PermissionDenied => continue,
                            Err(error) => return Err(error),
                        };
                        decisions.push(access);
                        let health = SearchService::cluster_health(
                            member,
                            Request::new(ClusterHealthRequest {
                                collection: name.clone(),
                            }),
                        )
                        .await;
                        if let Some(access) = decisions.last().and_then(Option::as_ref) { access.check()?; }
                        let health = health?.into_inner();
                        collections.push(CollectionHealth {
                            name: name.clone(),
                            health: Some(health),
                        });
                    }
                    if principal.is_some() && collections.is_empty() {
                        return Err(Status::permission_denied("no authorized collections"));
                    }
                    for access in decisions.iter().flatten() { access.check()?; }
                    return Ok(Response::new(ClusterHealthResponse {
                        targets: Vec::new(),
                        clustered_vector: None,
                        collections,
                        provider_mismatch: String::new(),
                        topology_generation: 0,
                    }));
                }
                let (name, target, access) = self.resolve_authorized(principal.as_ref(), &requested, AccessAction::Admin)?;
                let target = target.clone();
                request.get_mut().collection = name;
                let response = SearchService::cluster_health(&target, request).await;
                if let Some(access) = &access { access.check()?; }
                response
            }
        }
    };
}

search_service_over_collections! {
    search [Search]: SearchRequest => SearchResponse,
    bm25_search [Search]: Bm25SearchRequest => Bm25SearchResponse,
    phrase_search [Search]: PhraseSearchRequest => Bm25SearchResponse,
    hybrid_search [Search]: HybridSearchRequest => HybridSearchResponse,
    broadcast_vector_backend [Admin]: BroadcastVectorBackendRequest => BroadcastVectorBackendResponse,
    variant_search [Search]: VariantSearchRequest => VariantSearchResponse,
    query [Search]: QueryRequest => QueryResponse,
    plan_index [Admin]: PlanIndexRequest => PlanIndexResponse,
    describe_schema [Admin]: DescribeSchemaRequest => DescribeSchemaResponse,
    freeze_topology_writes [Admin]: FreezeTopologyWritesRequest => FreezeTopologyWritesResponse,
    publish_topology [Admin]: PublishTopologyRequest => PublishTopologyResponse,
    abort_topology_cutover [Admin]: AbortTopologyCutoverRequest => AbortTopologyCutoverResponse,
    aggregate [Search]: AggregateRequest => AggregateResponse,
    suggest [Search]: SuggestRequest => SuggestResponse,
    plan_placement [Admin]: PlanPlacementRequest => PlanPlacementResponse,
    term_suggest [Search]: TermSuggestRequest => TermSuggestResponse,
}
