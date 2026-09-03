//! Collections (`docs/collections.md`): one cluster, many datasets, no
//! bleed between them.
//!
//! A [`CoordinatorServiceImpl`] already owns everything one dataset needs
//! — its shard set and topology, per-shard statistics cache, calibration
//! and vector backend, BM25 parameters, analysis backend, phrase index,
//! quality profile, limits. A collection IS one of those, under a name.
//! This module is the layer that contains several of them and routes each
//! public request to the one it names, so a request can get to one
//! collection's shards and statistics and no other's by construction:
//! there is no shared table to leak through.
//!
//! The same shape contains for cluster control: one durable plane per
//! collection, dispatched by the name every control request carries.
//!
//! Naming rules, applied once here and enforced again by each member
//! (`CoordinatorServiceImpl::admit`, `ClusterControlService::admit`):
//!
//! - A set built from one unnamed coordinator (`single`) serves requests
//!   with an empty name and refuses any named one: it has no collection
//!   of that name, and says so.
//! - A set of named collections serves a named request from that member,
//!   refuses an unknown name, and returns an unnamed request from the
//!   configured default only — with no default it refuses, naming the
//!   collections it contains. An unnamed request is never routed to
//!   "whichever" dataset.

use std::collections::BTreeMap;

use tonic::{Request, Response, Status, Streaming};

use crate::control_plane::ClusterControlService;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::cluster_control_server::{ClusterControl, ClusterControlServer};
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    AbortTopologyCutoverRequest, AbortTopologyCutoverResponse, AggregateRequest, AggregateResponse,
    Bm25SearchRequest, Bm25SearchResponse, BroadcastCalibrationRequest,
    BroadcastCalibrationResponse, BroadcastVectorBackendRequest, BroadcastVectorBackendResponse,
    ClusterHealthRequest, ClusterHealthResponse, ClusterPlan, CollectionHealth,
    CompletePlacementActionRequest, DrainNodeRequest, FreezeTopologyWritesRequest,
    FreezeTopologyWritesResponse, GetClusterPlanRequest, HybridSearchRequest, HybridSearchResponse,
    NodeLease, PhraseSearchRequest, PlanIndexRequest, PlanIndexResponse, PublishTopologyRequest,
    PublishTopologyResponse, QueryRequest, QueryResponse, QueryStreamRequest,
    ReconcileClusterRequest, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    RollbackClusterRequest, RoutedIngestMappedRequest, RoutedIngestMappedResponse, SearchRequest,
    SearchResponse, VariantSearchRequest, VariantSearchResponse,
};

/// The membership and naming rules, shared by the search and control
/// sets.
#[derive(Debug, Clone)]
struct Members<T: Clone> {
    named: BTreeMap<String, T>,
    unnamed: Option<T>,
    default: Option<String>,
}

impl<T: Clone> Members<T> {
    fn single(member: T) -> Self {
        Members {
            named: BTreeMap::new(),
            unnamed: Some(member),
            default: None,
        }
    }

    fn named(
        members: Vec<(String, T)>,
        default: Option<String>,
        what: &str,
    ) -> Result<Self, String> {
        if members.is_empty() {
            return Err(format!(
                "{what}: a named collection set needs at least one collection"
            ));
        }
        let mut named = BTreeMap::new();
        for (name, member) in members {
            validate_name(&name)?;
            if named.insert(name.clone(), member).is_some() {
                return Err(format!("{what}: collection {name:?} is declared twice"));
            }
        }
        if let Some(default) = &default {
            if !named.contains_key(default) {
                return Err(format!(
                    "{what}: default collection {default:?} is not one of {:?}",
                    named.keys().collect::<Vec<_>>()
                ));
            }
        }
        Ok(Members {
            named,
            unnamed: None,
            default,
        })
    }

    fn names(&self) -> Vec<&str> {
        self.named.keys().map(String::as_str).collect()
    }

    fn resolve(&self, requested: &str) -> Result<(String, &T), Status> {
        if requested.is_empty() {
            if let Some(member) = &self.unnamed {
                return Ok((String::new(), member));
            }
            return match &self.default {
                Some(default) => Ok((default.clone(), &self.named[default])),
                None => Err(Status::invalid_argument(format!(
                    "no collection named: this coordinator serves the collections {:?} and has \
                     no default; name one in `collection`",
                    self.names()
                ))),
            };
        }
        if let Some(member) = self.named.get(requested) {
            return Ok((requested.to_string(), member));
        }
        Err(if self.unnamed.is_some() {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}: this coordinator serves one unnamed dataset \
                 and no named collections"
            ))
        } else {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}; this coordinator serves {:?}",
                self.names()
            ))
        })
    }
}

/// A collection name: non-empty, printable ASCII without whitespace or
/// path separators, at most 128 bytes — a name an operator can put in a
/// flag, a directory, and a metric label unchanged.
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("collection name is empty".to_string());
    }
    if name.len() > 128 {
        return Err(format!("collection name {name:?} is longer than 128 bytes"));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_graphic() && !matches!(c, '/' | '\\' | ':' | '"' | '\'')))
    {
        return Err(format!(
            "collection name {name:?} contains {bad:?}; names are printable ASCII without \
             whitespace, quotes, colons, or slashes"
        ));
    }
    Ok(())
}

/// The public search surface over one or more collections.
#[derive(Clone)]
pub struct CollectionSet {
    members: Members<CoordinatorServiceImpl>,
}

impl CollectionSet {
    /// One unnamed dataset: the shape every pre-collection deployment has.
    pub fn single(coordinator: CoordinatorServiceImpl) -> Self {
        CollectionSet {
            members: Members::single(coordinator),
        }
    }

    /// Named collections. Each coordinator must have been built for the
    /// name it is registered under (`with_collection`), no node address
    /// may serve two collections, and the default, when given, must be a
    /// member.
    pub fn named(
        members: Vec<(String, CoordinatorServiceImpl)>,
        default: Option<String>,
    ) -> Result<Self, String> {
        for (name, coordinator) in &members {
            if coordinator.collection() != name {
                return Err(format!(
                    "collection {name:?}: its coordinator was built for {:?}",
                    coordinator.collection()
                ));
            }
        }
        let mut owners: BTreeMap<String, String> = BTreeMap::new();
        for (name, coordinator) in &members {
            for addr in coordinator.node_addresses() {
                if let Some(other) = owners.insert(addr.clone(), name.clone()) {
                    if other != *name {
                        return Err(format!(
                            "node {addr} is listed under collections {other:?} and {name:?}; a \
                             shard belongs to only one collection"
                        ));
                    }
                }
            }
        }
        Ok(CollectionSet {
            members: Members::named(members, default, "collections")?,
        })
    }

    /// The collection names this set serves (empty for the unnamed set).
    pub fn names(&self) -> Vec<&str> {
        self.members.names()
    }

    /// The coordinator a request naming `collection` gets to.
    pub fn resolve(&self, collection: &str) -> Result<&CoordinatorServiceImpl, Status> {
        self.members.resolve(collection).map(|(_, member)| member)
    }

    /// Every member's coordinator, by name ("" for the unnamed dataset).
    pub fn members(&self) -> Vec<(&str, &CoordinatorServiceImpl)> {
        let mut out: Vec<(&str, &CoordinatorServiceImpl)> = self
            .members
            .named
            .iter()
            .map(|(name, member)| (name.as_str(), member))
            .collect();
        if let Some(member) = &self.members.unnamed {
            out.push(("", member));
        }
        out
    }

    /// Ask every node of every collection which collection it serves and
    /// refuse the set when any answer disagrees with the coordinator that
    /// lists it. Run at startup so a misconfigured fleet never serves.
    pub async fn verify_membership(&self) -> Result<(), Status> {
        for (_, member) in self.members() {
            member.verify_collection_membership().await?;
        }
        Ok(())
    }

    pub fn into_server(self, max_message_bytes: usize) -> SearchServiceServer<Self> {
        SearchServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}

/// The `SearchService` impl, produced by one macro so the `async_trait`
/// attribute sees every method: each unary request goes to the member
/// its `collection` names, with the resolved name written on it so the
/// member's own admission check sees the same answer.
macro_rules! search_service_over_collections {
    ($( $name:ident : $req:ty => $resp:ty ),* $(,)?) => {
        #[tonic::async_trait]
        impl SearchService for CollectionSet {
            type QueryStreamStream = <CoordinatorServiceImpl as SearchService>::QueryStreamStream;

            $(
                async fn $name(
                    &self,
                    mut request: Request<$req>,
                ) -> Result<Response<$resp>, Status> {
                    let (name, target) = self.members.resolve(&request.get_ref().collection)?;
                    let target = target.clone();
                    request.get_mut().collection = name;
                    SearchService::$name(&target, request).await
                }
            )*

            async fn query_stream(
                &self,
                mut request: Request<QueryStreamRequest>,
            ) -> Result<Response<Self::QueryStreamStream>, Status> {
                let (name, target) = self.members.resolve(&request.get_ref().collection)?;
                let target = target.clone();
                request.get_mut().collection = name;
                SearchService::query_stream(&target, request).await
            }

            async fn routed_ingest_mapped(
                &self,
                request: Request<Streaming<RoutedIngestMappedRequest>>,
            ) -> Result<Response<RoutedIngestMappedResponse>, Status> {
                let mut inbound = request.into_inner();
                let mut bind = CoordinatorServiceImpl::routed_bind(&mut inbound).await?;
                let (name, target) = self.members.resolve(&bind.collection)?;
                let target = target.clone();
                bind.collection = name;
                target
                    .routed_ingest_mapped_bound(bind, inbound)
                    .await
                    .map(Response::new)
            }

            async fn cluster_health(
                &self,
                mut request: Request<ClusterHealthRequest>,
            ) -> Result<Response<ClusterHealthResponse>, Status> {
                let requested = request.get_ref().collection.clone();
                // An unnamed health request on a named set lists every
                // collection separately; counts are never summed across them.
                if requested.is_empty() && self.members.unnamed.is_none() {
                    let mut collections = Vec::with_capacity(self.members.named.len());
                    for (name, member) in &self.members.named {
                        let health = SearchService::cluster_health(
                            member,
                            Request::new(ClusterHealthRequest {
                                collection: name.clone(),
                            }),
                        )
                        .await?
                        .into_inner();
                        collections.push(CollectionHealth {
                            name: name.clone(),
                            health: Some(health),
                        });
                    }
                    return Ok(Response::new(ClusterHealthResponse {
                        targets: Vec::new(),
                        clustered_vector: None,
                        collections,
                    }));
                }
                let (name, target) = self.members.resolve(&requested)?;
                let target = target.clone();
                request.get_mut().collection = name;
                SearchService::cluster_health(&target, request).await
            }
        }
    };
}

search_service_over_collections! {
    search: SearchRequest => SearchResponse,
    bm25_search: Bm25SearchRequest => Bm25SearchResponse,
    phrase_search: PhraseSearchRequest => Bm25SearchResponse,
    hybrid_search: HybridSearchRequest => HybridSearchResponse,
    broadcast_vector_backend: BroadcastVectorBackendRequest => BroadcastVectorBackendResponse,
    broadcast_calibration: BroadcastCalibrationRequest => BroadcastCalibrationResponse,
    variant_search: VariantSearchRequest => VariantSearchResponse,
    query: QueryRequest => QueryResponse,
    plan_index: PlanIndexRequest => PlanIndexResponse,
    freeze_topology_writes: FreezeTopologyWritesRequest => FreezeTopologyWritesResponse,
    publish_topology: PublishTopologyRequest => PublishTopologyResponse,
    abort_topology_cutover: AbortTopologyCutoverRequest => AbortTopologyCutoverResponse,
    aggregate: AggregateRequest => AggregateResponse,
}

/// The cluster-control surface over one durable plane per collection.
#[derive(Clone)]
pub struct ClusterControlSet {
    members: Members<ClusterControlService>,
}

impl ClusterControlSet {
    pub fn single(control: ClusterControlService) -> Self {
        ClusterControlSet {
            members: Members::single(control),
        }
    }

    pub fn named(
        members: Vec<(String, ClusterControlService)>,
        default: Option<String>,
    ) -> Result<Self, String> {
        for (name, control) in &members {
            if control.collection() != *name {
                return Err(format!(
                    "collection {name:?}: its control plane was opened for {:?}",
                    control.collection()
                ));
            }
        }
        Ok(ClusterControlSet {
            members: Members::named(members, default, "control planes")?,
        })
    }

    pub fn resolve(&self, collection: &str) -> Result<&ClusterControlService, Status> {
        self.members.resolve(collection).map(|(_, member)| member)
    }

    pub fn members(&self) -> Vec<(&str, &ClusterControlService)> {
        let mut out: Vec<(&str, &ClusterControlService)> = self
            .members
            .named
            .iter()
            .map(|(name, member)| (name.as_str(), member))
            .collect();
        if let Some(member) = &self.members.unnamed {
            out.push(("", member));
        }
        out
    }

    pub fn into_server(self, max_message_bytes: usize) -> ClusterControlServer<Self> {
        ClusterControlServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}

macro_rules! cluster_control_over_collections {
    ($( $name:ident : $req:ty => $resp:ty ),* $(,)?) => {
        #[tonic::async_trait]
        impl ClusterControl for ClusterControlSet {
            $(
                async fn $name(
                    &self,
                    mut request: Request<$req>,
                ) -> Result<Response<$resp>, Status> {
                    let (name, target) = self.members.resolve(&request.get_ref().collection)?;
                    let target = target.clone();
                    request.get_mut().collection = name;
                    ClusterControl::$name(&target, request).await
                }
            )*
        }
    };
}

cluster_control_over_collections! {
    register_node: RegisterNodeRequest => NodeLease,
    renew_node_lease: RenewNodeLeaseRequest => NodeLease,
    drain_node: DrainNodeRequest => ClusterPlan,
    report_shard: ReportShardRequest => ClusterPlan,
    complete_placement_action: CompletePlacementActionRequest => ClusterPlan,
    reconcile_cluster: ReconcileClusterRequest => ClusterPlan,
    get_cluster_plan: GetClusterPlanRequest => ClusterPlan,
    rollback_cluster: RollbackClusterRequest => ClusterPlan,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_operator_safe() {
        assert!(validate_name("court-opinions_v2.2026").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name("a:b").is_err());
        assert!(validate_name("naïve").is_err());
        assert!(validate_name(&"x".repeat(129)).is_err());
    }

    #[test]
    fn resolution_never_picks_silently() {
        let unnamed: Members<u8> = Members::single(1);
        assert_eq!(unnamed.resolve("").unwrap(), (String::new(), &1));
        assert!(unnamed
            .resolve("a")
            .unwrap_err()
            .message()
            .contains("one unnamed dataset"));

        let named: Members<u8> =
            Members::named(vec![("a".into(), 1), ("b".into(), 2)], None, "t").unwrap();
        assert_eq!(named.resolve("a").unwrap(), ("a".to_string(), &1));
        assert_eq!(named.resolve("b").unwrap(), ("b".to_string(), &2));
        let error = named.resolve("").unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("[\"a\", \"b\"]") && error.message().contains("no default")
        );
        let error = named.resolve("c").unwrap_err();
        assert!(error.message().contains("unknown collection \"c\""));

        let with_default: Members<u8> = Members::named(
            vec![("a".into(), 1), ("b".into(), 2)],
            Some("b".into()),
            "t",
        )
        .unwrap();
        assert_eq!(with_default.resolve("").unwrap(), ("b".to_string(), &2));

        assert!(
            Members::<u8>::named(vec![("a".into(), 1), ("a".into(), 2)], None, "t")
                .unwrap_err()
                .contains("declared twice")
        );
        assert!(
            Members::<u8>::named(vec![("a".into(), 1)], Some("z".into()), "t")
                .unwrap_err()
                .contains("not one of")
        );
        assert!(Members::<u8>::named(Vec::new(), None, "t").is_err());
    }
}
