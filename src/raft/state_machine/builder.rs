//! Building a snapshot of the store.

use super::*;

impl RaftSnapshotBuilder<ControlRaft> for ControlSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<ControlRaft>, StorageError<NodeId>> {
        self.build()
            .map_err(|e| snapshot_error(ErrorVerb::Write, e))
    }
}

impl ControlSnapshotBuilder {
    pub(super) fn build(&self) -> Result<Snapshot<ControlRaft>, Status> {
        let _serial = self.snapshot_lock.lock().unwrap();
        let store = self
            .store
            .read()
            .map_err(|_| Status::internal("state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("state machine store is being replaced"))?;
        let build_dir = self.snapshots.join(format!(
            "build-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir(&build_dir).map_err(io)?;
        let image = build_dir.join(IMAGE);
        let outcome = (|| -> Result<Snapshot<ControlRaft>, Status> {
            let applied = store.quiesced(|path, applied| {
                std::fs::copy(path, &image).map_err(io)?;
                std::fs::File::open(&image)
                    .and_then(|f| f.sync_all())
                    .map_err(io)?;
                Ok(applied)
            })?;
            let Some(applied) = applied else {
                return Err(Status::failed_precondition(
                    "no applied position; nothing to snapshot",
                ));
            };
            let last_log_id = applied.last_applied.as_ref().map(log_id_from_proto);
            let membership = stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("applied position has no membership"))?,
            )?;
            let signature = image_signature(&image, u64::MAX)?;
            let meta = SnapshotMeta {
                last_log_id,
                last_membership: membership,
                snapshot_id: format_snapshot_id(last_log_id.as_ref(), &signature),
            };
            write_meta(&build_dir, &self.identity, &meta, &signature)?;
            let generation = next_generation(&self.snapshots)?;
            let target = self
                .snapshots
                .join(GENERATIONS)
                .join(generation.to_string());
            std::fs::rename(&build_dir, &target).map_err(io)?;
            sync_dir(&self.snapshots.join(GENERATIONS))?;
            #[cfg(any(test, feature = "fault-injection"))]
            self.gates.pause_build();
            publish(&self.pointer_lock, &self.snapshots, generation)?;
            if let Ok(mut last) = self.last_build.lock() {
                *last = Some(SnapshotBuildReport {
                    generation,
                    bytes: signature.length,
                    receiver_bound: self.receiver_bound,
                    last_log_id,
                });
            }
            let file = std::fs::File::open(target.join(IMAGE)).map_err(io)?;
            Ok(Snapshot {
                meta,
                snapshot: Box::new(SnapshotFile::published(file)),
            })
        })();
        if outcome.is_err() {
            let _ = std::fs::remove_dir_all(&build_dir);
        }
        outcome
    }
}
