//! Process wiring of the `pipestream-search` binary: the serving roles
//! and the operator subcommands, one focused module each.

mod backend;
mod collections;
mod corpus;
mod corpus_tasks;
mod demo;
mod listeners;
mod membership;
mod node_role;
mod raft_member;
mod raft_ops;
mod run;
mod transport;
mod turbo;

use std::sync::Arc;

use pipestream_search::config::Config;
use pipestream_search::phrases::PhraseIndex;

pub(crate) use backend::configure_backend;
pub(crate) use raft_ops::{raft_operator_cli, raft_prepare};
pub(crate) use run::run;

/// A background task of the process; `run` joins every one before exit.
type TaskHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// What the listeners and background tasks of one process start share.
struct Startup<'a> {
    cfg: &'a Config,
    /// The phrase vocabulary every node and coordinator analyzes with.
    phrase_index: Option<Arc<PhraseIndex>>,
    /// The index-generation handshake pinned by `--turbo-model-sha256`.
    turbo_handshake: Option<turbo_client::IndexGenerationHandshake>,
    /// Raised once by SIGINT or SIGTERM.
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    /// The `(host, port)` a registered node advertises.
    advertise: Option<(String, u16)>,
    /// Woken by every configured shard's flush.
    flush_notify: Arc<tokio::sync::Notify>,
}
