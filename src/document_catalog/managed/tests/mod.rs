//! The managed catalog: binding a prepared owner, confirming readiness,
//! activating under the committed fence, and settling write outcomes.
mod activation;
mod activation_crash;
mod active_fixtures;
mod binding;
mod binding_crash;
mod binding_refusals;
mod fixtures;
mod outcomes;
mod publication;
mod publication_fixtures;
mod readiness;

use super::*;
use crate::test_support::ForkGuarded;
use crate::{
    authorization::{AccessPermit, Authorizer, PolicyAuthority},
    pb::{
        accept_document_request::Mutation, storage::source_authority_command::Action, storage::*,
        AcceptDocumentRequest, AccessAction, AccessPolicy, CollectionGrant, CollectionResource,
        ProtobufSource,
    },
    source_authority::SourceAuthorityStore,
};
use prost::Message;
use redb::{ReadableTable, ReadableTableMetadata};
use std::{path::PathBuf, sync::Arc};
use tonic::Code;

use active_fixtures::*;
use fixtures::*;
