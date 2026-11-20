mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    authorization::{Authorizer, PolicyAuthority},
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    link::NodeLink,
    node::{NodeConfig, NodeServiceImpl},
    pb::{search_service_server::SearchService, *},
    security::{PrincipalConfig, Principals},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tonic::{Code, Request, Status};

#[path = "field_grants/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "field_grants/aggregates_and_public_query.rs"]
mod aggregates_and_public_query;
#[path = "field_grants/dictionaries_phrases_and_numerics.rs"]
mod dictionaries_phrases_and_numerics;
#[path = "field_grants/document_visibility_views.rs"]
mod document_visibility_views;
#[path = "field_grants/integer_map_grants.rs"]
mod integer_map_grants;
#[path = "field_grants/policy_and_lexical_disclosure.rs"]
mod policy_and_lexical_disclosure;
#[path = "field_grants/streaming_boolean_and_revocation.rs"]
mod streaming_boolean_and_revocation;
