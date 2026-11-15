//! What a member records of the peers and snapshots it refused.

use super::*;

impl PeerRejections {
    pub(super) fn new() -> Self {
        let (sender, _) = tokio::sync::watch::channel(BTreeMap::new());
        Self {
            sender: Arc::new(sender),
        }
    }

    pub(super) fn record(&self, target: NodeId, action: &'static str, status: &Status) {
        self.sender.send_modify(|rejections| {
            let count = rejections.get(&target).map_or(0, |r| r.count) + 1;
            rejections.insert(
                target,
                PeerRejection {
                    action,
                    code: status.code(),
                    message: status.message().to_string(),
                    at: std::time::Instant::now(),
                    count,
                },
            );
        });
    }

    /// The latest rejection from each peer.
    pub fn latest(&self) -> BTreeMap<NodeId, PeerRejection> {
        self.sender.borrow().clone()
    }

    pub(crate) fn subscribe(
        &self,
    ) -> tokio::sync::watch::Receiver<BTreeMap<NodeId, PeerRejection>> {
        self.sender.subscribe()
    }
}

impl SnapshotRejections {
    pub(super) fn record(&self, from: NodeId, announced: u64, bound: u64) {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut last) = self.last.lock() {
            *last = Some(SnapshotRejection {
                from,
                announced,
                bound,
            });
        }
    }

    /// Installs rejected at the announce since this node started.
    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The latest rejection.
    pub fn last(&self) -> Option<SnapshotRejection> {
        self.last.lock().ok().and_then(|last| last.clone())
    }
}
