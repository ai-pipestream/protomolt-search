//! Peer admission and the persisted authority decision.

use prost::Message;
use redb::{Durability, ReadableTable};
use tonic::Status;

#[cfg(any(test, feature = "tls"))]
use super::frame::validate_frame;
#[cfg(feature = "tls")]
use super::hash;
use super::{decode, storage, ReplayJournal, FRAMES, META};
use crate::pb::storage::ReplayJournalHeader;
use crate::pb::ReplayAdmissionPolicy;
#[cfg(any(test, feature = "tls"))]
use crate::pb::{ReplayJournalFrame, ReplayJournalReceipt};

impl ReplayJournal {
    /// Admit an mTLS request using the verified leaf certificate supplied by
    /// tonic's transport. No header, bearer token or caller-supplied digest is
    /// an authenticated peer. The listener must verify its configured client CA.
    #[cfg(feature = "tls")]
    pub fn accept_authenticated(
        &self,
        request: &tonic::Request<ReplayJournalFrame>,
    ) -> Result<ReplayJournalReceipt, Status> {
        let certificates = request.peer_certs().ok_or_else(|| {
            Status::unauthenticated("replay requires a verified client certificate")
        })?;
        let certificate = certificates.first().ok_or_else(|| {
            Status::unauthenticated("replay requires a verified client certificate")
        })?;
        let fingerprint = hash(b"", certificate.as_ref());
        self.accept_inner(request.get_ref(), &fingerprint)
    }

    #[cfg(any(test, feature = "tls"))]
    pub(super) fn accept_inner(
        &self,
        frame: &ReplayJournalFrame,
        peer: &[u8],
    ) -> Result<ReplayJournalReceipt, Status> {
        validate_frame(&self.digest, frame)?;
        let encoded = frame.encode_to_vec();
        // Global lock order: authority then database. Keep the guard through
        // commit so session closure and admission have one ordered boundary.
        let authority = self.lock_authority()?;
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let mut frames = tx.open_table(FRAMES).map_err(storage)?;
            let mut header = self.checked_header(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                    .value(),
                &frames,
            )?;
            let policy = self.read_policy(&header, &meta)?;
            self.authorize_peer(policy.as_ref(), peer, header.admission_fenced)?;
            authority.check(self.authority_now())?;
            if frame.sequence <= header.accepted_sequence {
                let held = self.read_frame(&frames, frame.sequence)?;
                if held != *frame {
                    return Err(Status::already_exists(
                        "replay sequence already names different content",
                    ));
                }
                authority.check(self.authority_now())?;
                return Ok(self.receipt(frame, true));
            }
            if header.accepted_sequence.checked_add(1) != Some(frame.sequence)
                || frame.previous_sha256 != header.head_sha256
                || frame.source_clock <= header.source_clock
            {
                return Err(Status::failed_precondition("replay frame has a sequence gap, divergent chain or non-increasing source clock"));
            }
            frames
                .insert(frame.sequence, encoded.as_slice())
                .map_err(storage)?;
            header.accepted_sequence = frame.sequence;
            header.source_clock = frame.source_clock;
            header.head_sha256 = frame.sha256.clone();
            let header = header.encode_to_vec();
            meta.insert("header", header.as_slice()).map_err(storage)?;
        }
        authority.check(self.authority_now())?;
        tx.commit().map_err(storage)?;
        Ok(self.receipt(frame, false))
    }

    // Called only while holding the authority mutex; persistence cannot open
    // admission unless the live-session wrapper also confirms freshness.
    pub(super) fn persist_authorization(
        &self,
        policy: &ReplayAdmissionPolicy,
    ) -> Result<(), Status> {
        Self::validate_policy(policy)?;
        if policy.binding_sha256 != self.digest {
            return Err(Status::failed_precondition(
                "replay policy names another assignment",
            ));
        }
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let frames = tx.open_table(FRAMES).map_err(storage)?;
            let mut header = self.checked_header(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                    .value(),
                &frames,
            )?;
            if let Some(held) = self.read_policy(&header, &meta)? {
                if held.authority_id != policy.authority_id || policy.revision < held.revision {
                    return Err(Status::failed_precondition(
                        "replay authority incarnation differs or policy revision regressed",
                    ));
                }
                if held.revision == policy.revision {
                    if held != *policy {
                        return Err(Status::already_exists(
                            "replay policy revision already names another decision",
                        ));
                    }
                    return Ok(());
                }
            }
            if header.admission_fenced && policy.enabled && self.matches_live_assignment(policy) {
                return Err(Status::failed_precondition(
                    "replay assignment was permanently fenced; start a new assignment",
                ));
            }
            header.admission_fenced |= !self.matches_live_assignment(policy);
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert("admission", policy.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        tx.commit().map_err(storage)
    }

    fn validate_policy(policy: &ReplayAdmissionPolicy) -> Result<(), Status> {
        use crate::pb::NodeResidency;
        if policy.contract_version != 1
            || policy.authority_id.len() != 16
            || policy.revision == 0
            || policy.binding_sha256.len() != 32
            || policy.source_history_id.len() != 16
            || policy.target_history_id.len() != 16
            || policy.source_write_epoch == 0
            || policy.target_write_epoch == 0
            || policy.source_certificate_sha256.len() > 64
            || policy
                .source_certificate_sha256
                .iter()
                .any(|p| p.len() != 32)
        {
            return Err(Status::invalid_argument("invalid replay admission policy version, identities, digests, epochs or peer count"));
        }
        let mut peers = std::collections::BTreeSet::new();
        if policy
            .source_certificate_sha256
            .iter()
            .any(|p| !peers.insert(p))
        {
            return Err(Status::invalid_argument(
                "replay policy repeats a certificate fingerprint",
            ));
        }
        for residency in [policy.source_residency, policy.target_residency] {
            if !matches!(
                NodeResidency::try_from(residency),
                Ok(NodeResidency::Server | NodeResidency::Device)
            ) {
                return Err(Status::invalid_argument(
                    "replay policy requires explicit known source and target residency",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn read_policy(
        &self,
        header: &ReplayJournalHeader,
        meta: &impl ReadableTable<&'static str, &'static [u8]>,
    ) -> Result<Option<ReplayAdmissionPolicy>, Status> {
        let stored = meta
            .get("admission")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("replay journal admission record is missing"))?;
        if stored.value().is_empty() {
            if header.admission_fenced || header.accepted_sequence != 0 {
                return Err(Status::data_loss(
                    "replay journal holds a fence or history without an authority decision",
                ));
            }
            return Ok(None);
        }
        // Bound allocation before protobuf decode, including tampered files.
        if stored.value().len() > 8192 {
            return Err(Status::data_loss(
                "stored replay admission policy exceeds its bound",
            ));
        }
        let policy: ReplayAdmissionPolicy = decode(stored.value())?;
        Self::validate_policy(&policy).map_err(|e| Status::data_loss(e.message().to_string()))?;
        if policy.binding_sha256 != self.digest {
            return Err(Status::data_loss(
                "stored replay policy names another assignment",
            ));
        }
        if !header.admission_fenced && !self.matches_live_assignment(&policy) {
            return Err(Status::data_loss(
                "replay policy fencing observation lacks its permanent fence",
            ));
        }
        Ok(Some(policy))
    }

    fn matches_live_assignment(&self, policy: &ReplayAdmissionPolicy) -> bool {
        use crate::pb::NodeResidency;
        policy.source_residency == NodeResidency::Server as i32
            && policy.target_residency == NodeResidency::Server as i32
            && policy.source_history_id == self.binding.source_history_id
            && policy.target_history_id == self.binding.target_history_id
            && policy.source_write_epoch == self.binding.source_write_epoch
            && policy.target_write_epoch == self.binding.target_write_epoch
    }

    #[cfg(any(test, feature = "tls"))]
    fn authorize_peer(
        &self,
        policy: Option<&ReplayAdmissionPolicy>,
        peer: &[u8],
        permanently_fenced: bool,
    ) -> Result<(), Status> {
        let policy = policy.ok_or_else(|| {
            Status::permission_denied("replay assignment has no installed authority decision")
        })?;
        if permanently_fenced
            || !policy.enabled
            || !policy.source_certificate_sha256.iter().any(|p| p == peer)
            || !self.matches_live_assignment(policy)
        {
            return Err(Status::permission_denied(
                "replay peer or current assignment fencing state is not authorized",
            ));
        }
        Ok(())
    }
}
