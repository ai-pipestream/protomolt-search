//! Collections: one cluster, many datasets, no
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

use crate::authorization::{AccessPermit, AuthorizedStream};
use crate::error_disclosure::{DisclosedQueryStream, ErrorScope};
use crate::pb::AccessAction;
use std::collections::BTreeMap;

use tonic::{Request, Response, Status, Streaming};

use std::sync::Arc;

use crate::control_plane::ClusterControlService;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::cluster_control_server::{ClusterControl, ClusterControlServer};
use crate::pb::search_service_server::{SearchService, SearchServiceServer};
use crate::pb::{
    AbortTopologyCutoverRequest, AbortTopologyCutoverResponse, AggregateRequest, AggregateResponse,
    Bm25SearchRequest, Bm25SearchResponse, BroadcastVectorBackendRequest,
    BroadcastVectorBackendResponse, ClusterHealthRequest, ClusterHealthResponse, ClusterPlan,
    CollectionHealth, CompletePlacementActionRequest, DescribeSchemaRequest,
    DescribeSchemaResponse, DrainNodeRequest, FreezeTopologyWritesRequest,
    FreezeTopologyWritesResponse, GetClusterPlanRequest, HybridSearchRequest, HybridSearchResponse,
    NodeLease, OptimizeDensePolicyRequest, OptimizeDensePolicyResponse, PhraseSearchRequest,
    PlanBalanceRequest, PlanBalanceResponse, PlanIndexRequest, PlanIndexResponse,
    PlanPlacementRequest, PlanPlacementResponse, PublishTopologyRequest, PublishTopologyResponse,
    QueryRequest, QueryResponse, QueryStreamRequest, ReconcileClusterRequest, RegisterNodeRequest,
    RenewNodeLeaseRequest, ReportShardRequest, RollbackClusterRequest, RoutedIngestMappedRequest,
    RoutedIngestMappedResponse, SearchRequest, SearchResponse, SuggestRequest, SuggestResponse,
    TermSuggestRequest, TermSuggestResponse, VariantSearchRequest, VariantSearchResponse,
};
use crate::security::{Guarded, MeteredIngest, Permit, Principal, Principals};

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

mod control;
mod recent;
mod search_service;
mod search_set;
#[cfg(test)]
mod tests;

pub use self::control::ClusterControlSet;
use self::recent::*;
pub use self::search_set::CollectionSet;
