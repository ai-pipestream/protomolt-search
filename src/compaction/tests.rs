use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tonic::Status;

use crate::node::{Bm25Shard, NodeServiceImpl};
use crate::pb::{AddDocumentsRequest, CompactShardRequest, CompactShardResponse};
use crate::segments::SegmentCatalog;
use crate::vector::VectorIndex;
use crate::wal::{self, WalWriter};

use super::*;
use crate::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use crate::node::NodeConfig;
use crate::vector::EMBEDDED_TURBOVEC;
use prost::Message;
use std::sync::Arc;

mod shadow_flush;
mod walless;
mod walless_fixture;

#[tokio::test]
async fn a_reserved_cutover_yields_writers_without_blocking_reads() {
    use crate::pb::node_service_server::NodeService;
    let node = NodeServiceImpl::new(None, NodeConfig::default());
    let reservation = node.mutation_gate.clone().write_owned().await;
    let writer_node = node.clone();
    let (started, entered) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        started.send(()).unwrap();
        writer_node
            .delete_documents(tonic::Request::new(
                crate::pb::DeleteDocumentsRequest::default(),
            ))
            .await
    });
    entered.await.unwrap();
    tokio::task::yield_now().await;
    assert!(
        !writer.is_finished(),
        "write bypassed the cutover reservation"
    );
    node.health(tonic::Request::new(crate::pb::HealthRequest {}))
        .await
        .unwrap();
    drop(reservation);
    let deleted = writer.await.unwrap().unwrap().into_inner();
    assert_eq!(deleted.deleted, 0);
}
