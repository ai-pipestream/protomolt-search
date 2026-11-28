//! Writes in the migration exercise, ordered by gates instead of sleeps.
//!
//! The admission lease runs on the real clock, and the product decides
//! whether a commit finished inside it: `Accepted` when it did,
//! `Unconfirmed` when the commit became durable after the lease lapsed.
//! Both are correct outcomes, and which one a run sees depends on how fast
//! the host is. The exercise therefore never asserts that a write lands
//! inside the lease; it orders the events it does control with gates, and
//! settles an `Unconfirmed` write the way a caller must, under a fresh
//! lease, then asserts the same durable end state either way.

use std::sync::Arc;
use std::time::Duration;

use pipestream_search::document_catalog::{Acceptance, ActiveManagedCatalog, Settlement};
use pipestream_search::pb::{AcceptDocumentRequest, DocumentWriteReceipt};
use pipestream_search::raft::RaftHost;

use super::control_adversarial::raft_kit::Voters;

/// How a write's commit was judged against the lease it was admitted under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judged {
    InsideTheLease,
    AfterTheLeaseLapsed,
}

/// Bound on waits for an event the test itself causes. Reaching it is a
/// failure by name, never a slow pass.
const EVENT_CEILING: Duration = Duration::from_secs(30);

/// One write through `node`, settled under a fresh lease if its commit
/// outlasted the lease it was admitted under. The receipt is the durable
/// write's, whichever way it was judged.
pub async fn write_through(
    voters: &Voters,
    node: u64,
    catalog: &ActiveManagedCatalog,
    request: &AcceptDocumentRequest,
) -> (DocumentWriteReceipt, Judged) {
    let host = voters.host(node);
    let accepted = host
        .with_admission("alice", |admission| catalog.accept(admission, request))
        .await
        .expect("the write is admitted");
    let judged = judged(&accepted);
    let receipt = match accepted {
        Acceptance::Accepted(receipt) => receipt,
        Acceptance::Unconfirmed(receipt) => {
            settle(host, catalog, request, &receipt).await;
            receipt
        }
    };
    (receipt, judged)
}

/// Settle an `Unconfirmed` write under a fresh lease on `host` and require
/// it accepted with the receipt the write returned: nothing in the
/// exercise revokes alice's right, so a fence here is a defect.
pub async fn settle(
    host: &RaftHost,
    catalog: &ActiveManagedCatalog,
    request: &AcceptDocumentRequest,
    receipt: &DocumentWriteReceipt,
) {
    let settled = host
        .with_admission("alice", |admission| {
            catalog.settle(admission, "alice", &request.operation_id)
        })
        .await
        .expect("an unconfirmed write settles under a fresh lease");
    match settled {
        Settlement::Accepted(settled) => assert_eq!(&settled, receipt, "settled receipt"),
        Settlement::Fenced(rejection) => {
            panic!("an unconfirmed write was fenced with its right intact: {rejection}")
        }
    }
}

pub fn judged(acceptance: &Acceptance) -> Judged {
    match acceptance {
        Acceptance::Accepted(_) => Judged::InsideTheLease,
        Acceptance::Unconfirmed(_) => Judged::AfterTheLeaseLapsed,
    }
}

/// Start `request` on `host` and return once it is parked inside its
/// source transaction, after its lease was granted and every table write
/// and before its final admission check. `cut` runs while it is parked;
/// the write is then released and its outcome returned. A write that
/// finishes before it parks fails the test with its result.
pub async fn with_write_parked(
    host: Arc<RaftHost>,
    catalog: Arc<ActiveManagedCatalog>,
    request: AcceptDocumentRequest,
    cut: impl FnOnce(),
) -> Acceptance {
    let gate = catalog.arm_precommit_gate();
    let mut task = tokio::spawn(async move {
        host.with_admission("alice", |admission| catalog.accept(admission, &request))
            .await
    });
    tokio::time::timeout(EVENT_CEILING, async {
        tokio::select! {
            () = gate.reached() => {}
            finished = &mut task => {
                panic!("the write finished before it parked: {finished:?}")
            }
        }
    })
    .await
    .expect("the write parks inside its transaction");
    cut();
    gate.release();
    tokio::time::timeout(EVENT_CEILING, task)
        .await
        .expect("the released write returns")
        .expect("the write task ran to completion")
        .expect("the parked write is durable")
}
