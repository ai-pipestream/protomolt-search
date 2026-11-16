//! The access-controlled catalog: pinned permits through the source
//! commit, and the migration of legacy operations to actor scope.
mod actor_audit;
mod actor_migration;
mod fixtures;
mod pinning;

use super::*;
use crate::{
    authorization::{AccessPermit, AuthorizationGuard, Authorizer, PolicyAuthority},
    pb::{
        accept_document_request::Mutation,
        storage::{DocumentCatalogHeader, SourceResourceBinding},
        AcceptDocumentRequest, AccessAction, AccessDecision, AccessPolicy, CollectionGrant,
        CollectionResource, ProtobufSource,
    },
};
use prost::Message;
use redb::ReadableTable;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
use tonic::{Code, Status};

use fixtures::*;
