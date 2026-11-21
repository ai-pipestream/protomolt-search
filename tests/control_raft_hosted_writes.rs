//! Hosted owner writes: the network write service in which every admission
//! comes from the Raft host's lease, over three voters on loopback mTLS.
//!
//! A write admitted under a current
//! lease commits and stays durable; a lease that lapsed before the grant
//! admits nothing and leaves no row and no retry record; off-leader and
//! isolated members refuse by name.
//!
//! Compiled only with `raft`, `tls` and `fault-injection` together (the
//! `control_raft_admission` convention: the lease scenarios need the pause
//! hook `arm_grant_delay`, and the group needs the mTLS transport). The
//! write service itself serves plain loopback HTTP here with a bearer
//! principal; in production it registers on the process's secured listener.
#![cfg(all(feature = "raft", feature = "tls", feature = "fault-injection"))]
// Every test holds the target-wide `serial()` guard for its whole body,
// across awaits, on purpose.
#![allow(clippy::await_holding_lock)]

mod control_adversarial;

use std::sync::Arc;
use std::time::{Duration, Instant};

use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::{AccessControlledCatalog, ActiveManagedCatalog};
use pipestream_search::document_write_service::hosted::HostedDocumentWriteService;
use pipestream_search::pb::document_write_service_client::DocumentWriteServiceClient;
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, SourceAuthorityIdentity, SourceOwnerCompletion,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, AccessPolicy,
    DocumentWriteRequest, DocumentWriteServiceLimits, GetDocumentWriteTargetRequest,
    ProtobufSource, ReadAcceptedDocumentsRequest,
};
use pipestream_search::raft::reasons;
use pipestream_search::raft::RaftHost;
use tonic::Code;

use raft_kit::Voters;

use pipestream_search::config::RaftManagedCatalog;
use pipestream_search::document_write_service::hosted::recover_catalogs;

#[path = "control_raft_hosted_writes/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "control_raft_hosted_writes/catalog_restart_validation.rs"]
mod catalog_restart_validation;
#[path = "control_raft_hosted_writes/inflight_permits_and_cas.rs"]
mod inflight_permits_and_cas;
#[path = "control_raft_hosted_writes/leader_and_follower_admission.rs"]
mod leader_and_follower_admission;
#[path = "control_raft_hosted_writes/lease_expiry_and_isolation.rs"]
mod lease_expiry_and_isolation;
#[path = "control_raft_hosted_writes/settlement_and_fencing.rs"]
mod settlement_and_fencing;
#[path = "control_raft_hosted_writes/status_and_reconcile.rs"]
mod status_and_reconcile;
