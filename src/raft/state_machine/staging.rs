//! Where received images are staged, claimed, and verified.

use super::*;

impl SnapshotStaging {
    pub(crate) fn max_image_bytes(&self) -> u64 {
        self.max_image_bytes
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub(crate) fn gates(&self) -> &Arc<SnapshotGates> {
        &self.gates
    }

    pub(super) fn reserve(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        Arc::clone(&self.owned).try_acquire_owned().map_err(|_| Status::resource_exhausted(format!(
            "snapshot finalization is at its {MAX_OWNED_RECEIVES} image bound; retry after an owned receive completes"
        )))
    }

    pub(crate) fn claim(&self, dir: &Path, file: std::fs::File) -> Result<ClaimedReceive, Status> {
        let permit = match self.reserve() {
            Ok(permit) => permit,
            Err(error) => {
                drop(file);
                self.discard(dir);
                return Err(error);
            }
        };
        let mut receiving = self.receiving.lock().unwrap();
        if !receiving.as_ref().is_some_and(|receive| receive.dir == dir) {
            return Err(Status::failed_precondition(
                "the snapshot no longer owns its receive",
            ));
        }
        let receive = receiving.take().expect("the exact receive was checked");
        Ok(ClaimedReceive {
            file,
            receive: OwnedReceive {
                receive,
                _permit: permit,
                completion: None,
            },
        })
    }

    pub(super) fn own_reserved(
        &self,
        dir: &Path,
        file: std::fs::File,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<SnapshotFile, Status> {
        let mut receiving = self.receiving.lock().unwrap();
        if !receiving.as_ref().is_some_and(|receive| receive.dir == dir) {
            return Err(Status::failed_precondition(
                "the snapshot no longer owns its receive",
            ));
        }
        let receive = receiving.take().expect("the exact receive was checked");
        Ok(SnapshotFile {
            file: tokio::fs::File::from_std(file),
            receive: Some(OwnedReceive {
                receive,
                _permit: permit,
                completion: None,
            }),
        })
    }

    /// Begin a receive: a fresh directory with an empty image file. A
    /// previous unfinished receive is dropped with its bytes.
    pub(crate) fn begin(&self) -> Result<(PathBuf, std::fs::File), Status> {
        if let Some(previous) = self.receiving.lock().unwrap().take() {
            let _ = std::fs::remove_dir_all(&previous.dir);
        }
        let token = format!(
            "incoming-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let dir = self.snapshots.join(token);
        std::fs::create_dir(&dir).map_err(io)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.join(IMAGE))
            .map_err(io)?;
        *self.receiving.lock().unwrap() = Some(Receive {
            dir: dir.clone(),
            verified: None,
        });
        Ok((dir, file))
    }

    /// Validate the complete image in `dir` against `meta`: the announced
    /// bound, its length and digest, then a separate probe copy opened as a
    /// store of this group whose applied position and membership must
    /// equal the meta. The received bytes stay untouched and are marked
    /// verified for this meta.
    pub(crate) fn verify(
        &self,
        dir: &Path,
        meta: &SnapshotMeta<NodeId, BasicNode>,
    ) -> Result<ImageSignature, Status> {
        let image = dir.join(IMAGE);
        let signature = parse_snapshot_id(&meta.snapshot_id)?;
        if signature.length > self.max_image_bytes {
            return Err(Status::resource_exhausted(format!(
                "snapshot image of {} bytes exceeds the {} byte bound",
                signature.length, self.max_image_bytes
            )));
        }
        verify_image(&image, &signature, self.max_image_bytes)?;
        // Probe a separate copy: a redb open may rewrite recovery state, and
        // the received bytes must stay identical to their checksum.
        #[cfg(any(test, feature = "fault-injection"))]
        self.gates.pause_verify();
        let probe = dir.join(PROBE);
        std::fs::copy(&image, &probe).map_err(io)?;
        let checked = (|| -> Result<(), Status> {
            let opened = SourceAuthorityStore::open(&probe, &self.identity)
                .map_err(|e| Status::data_loss(format!("snapshot image: {}", e.message())))?;
            let applied = opened
                .raft_applied()?
                .ok_or_else(|| Status::data_loss("snapshot image carries no applied position"))?;
            if applied.last_applied.as_ref().map(log_id_from_proto) != meta.last_log_id {
                return Err(super::super::reasons::reason(
                    Status::data_loss(
                        "snapshot image applied position differs from the snapshot meta",
                    ),
                    super::super::reasons::TRANSPORT_SNAPSHOT_POSITION_MISMATCH,
                ));
            }
            let membership = stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("snapshot image carries no membership"))?,
            )?;
            if membership != meta.last_membership {
                return Err(super::super::reasons::reason(
                    Status::data_loss("snapshot image membership differs from the snapshot meta"),
                    super::super::reasons::TRANSPORT_SNAPSHOT_MEMBERSHIP_MISMATCH,
                ));
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(&probe);
        checked?;
        if let Some(receive) = self.receiving.lock().unwrap().as_mut() {
            if receive.dir == dir {
                receive.verified = Some(meta.snapshot_id.clone());
            }
        }
        Ok(signature)
    }

    /// Drop a receive and its bytes.
    pub(crate) fn discard(&self, dir: &Path) {
        let mut receiving = self.receiving.lock().unwrap();
        if receiving.as_ref().is_some_and(|r| r.dir == dir) {
            *receiving = None;
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
