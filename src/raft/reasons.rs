//! Stable reason identifiers for every named rejection the Raft
//! documents describe (`docs/raft-error-registry.md`). A rejection
//! carries its identifier as the `psearch-reason` metadata on the
//! `Status`, the way the transport marks its answers with
//! `psearch-raft-answer`: the gRPC code and the identifier are frozen,
//! the human message text is informative and may change. Tests assert
//! the code and the identifier, not sentences.
use tonic::metadata::MetadataValue;

/// Metadata key carrying one reason identifier on an answered `Status`.
pub const REASON: &str = "psearch-reason";

// Transport answers (`src/raft/transport.rs`).
pub const TRANSPORT_NO_CLIENT_CERTIFICATE: &str = "transport.no_client_certificate";
pub const TRANSPORT_UNREGISTERED_CERTIFICATE: &str = "transport.unregistered_certificate";
pub const TRANSPORT_BAD_HEADER: &str = "transport.bad_header";
pub const TRANSPORT_WRONG_GROUP: &str = "transport.wrong_group";
pub const TRANSPORT_MISADDRESSED: &str = "transport.misaddressed";
pub const TRANSPORT_IDENTITY_MISMATCH: &str = "transport.identity_mismatch";
pub const TRANSPORT_ISOLATED: &str = "transport.isolated";
pub const TRANSPORT_NOT_HEARD: &str = "transport.not_heard";
pub const TRANSPORT_TIMING_MISMATCH: &str = "transport.timing_mismatch";
pub const TRANSPORT_NO_LINEARIZABLE_READ: &str = "transport.no_linearizable_read";
pub const TRANSPORT_SNAPSHOT_OVER_BOUND: &str = "snapshot.over_bound";
pub const TRANSPORT_SNAPSHOT_DIGEST: &str = "snapshot.digest";
pub const TRANSPORT_SNAPSHOT_STALE_CONTINUATION: &str = "snapshot.stale_continuation";
pub const TRANSPORT_SNAPSHOT_TRANSFER_BOUND: &str = "snapshot.transfer_bound";
pub const TRANSPORT_SNAPSHOT_OVER_ANNOUNCED: &str = "snapshot.over_announced";
pub const TRANSPORT_SNAPSHOT_MEMBERSHIP_MISMATCH: &str = "snapshot.membership_mismatch";
pub const TRANSPORT_SNAPSHOT_POSITION_MISMATCH: &str = "snapshot.position_mismatch";

// Leases (`src/raft/host.rs`, `src/raft/transport.rs`, `src/source_authority.rs`).
pub const LEASE_NOT_LEADER: &str = "lease.not_leader";
pub const LEASE_NO_KNOWN_LEADER: &str = "lease.no_known_leader";
pub const LEASE_NO_TRANSPORT: &str = "lease.no_transport";
pub const LEASE_NO_LINEARIZABLE_READ: &str = "lease.no_linearizable_read";
pub const LEASE_LEADER_UNREACHABLE: &str = "lease.leader_unreachable";
pub const LEASE_READ_POSITION_BEHIND: &str = "lease.read_position_behind";
pub const LEASE_INTERVAL_ELAPSED: &str = "lease.interval_elapsed";

// Membership (`src/raft/host.rs`).
pub const MEMBERSHIP_NOT_NETWORKED: &str = "membership.not_networked";
pub const MEMBERSHIP_UNREGISTERED_CERTIFICATE: &str = "membership.unregistered_certificate";
pub const MEMBERSHIP_BAD_ADDRESS: &str = "membership.bad_address";
pub const MEMBERSHIP_NO_LEARNERS: &str = "membership.no_learners";
pub const MEMBERSHIP_CHANGE_REJECTED: &str = "membership.change_rejected";
pub const MEMBERSHIP_AWAITING_SNAPSHOT: &str = "membership.awaiting_snapshot";

// Write outcomes (`src/document_catalog/`).
pub const ADMISSION_EPOCH_MOVED: &str = "admission.epoch_moved";
pub const ADMISSION_NOT_ACTIVE: &str = "admission.not_active";
pub const ADMISSION_FOREIGN_AUTHORITY: &str = "admission.foreign_authority";
pub const ADMISSION_FOREIGN_COLLECTION: &str = "admission.foreign_collection";
pub const ADMISSION_UNRECORDED_ACTIVATION: &str = "admission.unrecorded_activation";
pub const OUTCOME_FENCED: &str = "outcome.fenced";
pub const OUTCOME_NO_OPERATION: &str = "outcome.no_operation";
pub const OUTCOME_UNKNOWN_OUTCOME: &str = "outcome.unknown_outcome";
pub const OUTCOME_VERSION_UNKNOWN: &str = "outcome.version_unknown";
pub const OUTCOME_VERSION_MISMATCH: &str = "outcome.version_mismatch";
pub const OUTCOME_OPERATION_REUSED: &str = "outcome.operation_reused";
pub const OUTCOME_UNCONFIRMED: &str = "outcome.unconfirmed";

// Receipt delivery (`src/error_disclosure.rs`, served from the hosted path).
pub const RECEIPT_POLICY_CHANGED: &str = "receipt.policy_changed";

// Operator routes. Provisional: the service lands in Phase A, and these
// rows are reserved for it; no site attaches them on this base.
pub const OPERATOR_NO_MEMBERSHIP: &str = "operator.no_membership";
pub const OPERATOR_NOT_LEADER: &str = "operator.not_leader";

/// Attach a reason identifier to a rejection `Status`. Message text is
/// unchanged; only the metadata is added.
pub fn reason(mut status: tonic::Status, identifier: &'static str) -> tonic::Status {
    status
        .metadata_mut()
        .insert(REASON, MetadataValue::from_static(identifier));
    status
}

/// The identifier a rejection `Status` carries, if it carries one.
pub fn reason_of(status: &tonic::Status) -> Option<&str> {
    status.metadata().get(REASON)?.to_str().ok()
}

/// Carry the identifier of `from`, if it has one, onto `to`: a wrapper
/// that rewords a rejection keeps the code and the identifier the site
/// set, so a reason attached deep in a call survives the reworded answer.
pub fn carry(from: &tonic::Status, mut to: tonic::Status) -> tonic::Status {
    if let Some(value) = from.metadata().get(REASON) {
        to.metadata_mut().insert(REASON, value.clone());
    }
    to
}

/// Every identifier constant in this module, for the registry equality
/// test: the document's identifier column and this list cannot drift.
pub const ALL: &[&str] = &[
    TRANSPORT_NO_CLIENT_CERTIFICATE,
    TRANSPORT_UNREGISTERED_CERTIFICATE,
    TRANSPORT_BAD_HEADER,
    TRANSPORT_WRONG_GROUP,
    TRANSPORT_MISADDRESSED,
    TRANSPORT_IDENTITY_MISMATCH,
    TRANSPORT_ISOLATED,
    TRANSPORT_NOT_HEARD,
    TRANSPORT_TIMING_MISMATCH,
    TRANSPORT_NO_LINEARIZABLE_READ,
    TRANSPORT_SNAPSHOT_OVER_BOUND,
    TRANSPORT_SNAPSHOT_DIGEST,
    TRANSPORT_SNAPSHOT_STALE_CONTINUATION,
    TRANSPORT_SNAPSHOT_TRANSFER_BOUND,
    TRANSPORT_SNAPSHOT_OVER_ANNOUNCED,
    TRANSPORT_SNAPSHOT_MEMBERSHIP_MISMATCH,
    TRANSPORT_SNAPSHOT_POSITION_MISMATCH,
    LEASE_NOT_LEADER,
    LEASE_NO_KNOWN_LEADER,
    LEASE_NO_TRANSPORT,
    LEASE_NO_LINEARIZABLE_READ,
    LEASE_LEADER_UNREACHABLE,
    LEASE_READ_POSITION_BEHIND,
    LEASE_INTERVAL_ELAPSED,
    MEMBERSHIP_NOT_NETWORKED,
    MEMBERSHIP_UNREGISTERED_CERTIFICATE,
    MEMBERSHIP_BAD_ADDRESS,
    MEMBERSHIP_NO_LEARNERS,
    MEMBERSHIP_CHANGE_REJECTED,
    MEMBERSHIP_AWAITING_SNAPSHOT,
    ADMISSION_EPOCH_MOVED,
    ADMISSION_NOT_ACTIVE,
    ADMISSION_FOREIGN_AUTHORITY,
    ADMISSION_FOREIGN_COLLECTION,
    ADMISSION_UNRECORDED_ACTIVATION,
    OUTCOME_FENCED,
    OUTCOME_NO_OPERATION,
    OUTCOME_UNKNOWN_OUTCOME,
    OUTCOME_VERSION_UNKNOWN,
    OUTCOME_VERSION_MISMATCH,
    OUTCOME_OPERATION_REUSED,
    OUTCOME_UNCONFIRMED,
    RECEIPT_POLICY_CHANGED,
    OPERATOR_NO_MEMBERSHIP,
    OPERATOR_NOT_LEADER,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_attaches_and_reads_back() {
        let status = reason(tonic::Status::unavailable("some words"), LEASE_NOT_LEADER);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "some words");
        assert_eq!(reason_of(&status), Some(LEASE_NOT_LEADER));
        assert_eq!(reason_of(&tonic::Status::unavailable("bare")), None,);
        // A reworded wrapper carries the identifier and its own code and
        // message; a source without one adds nothing.
        let carried = carry(&status, tonic::Status::aborted("reworded"));
        assert_eq!(carried.code(), tonic::Code::Aborted);
        assert_eq!(carried.message(), "reworded");
        assert_eq!(reason_of(&carried), Some(LEASE_NOT_LEADER));
        let bare = carry(
            &tonic::Status::unavailable("bare"),
            tonic::Status::aborted("reworded"),
        );
        assert_eq!(reason_of(&bare), None);
    }

    /// `ALL` names every identifier constant declared in this file, so a
    /// constant added without its entry fails here and the registry test
    /// below sees every one. The declarations are read from the source.
    #[test]
    fn every_identifier_constant_is_listed() {
        let mut declared = Vec::new();
        for line in include_str!("reasons.rs").lines() {
            let Some(rest) = line.trim().strip_prefix("pub const ") else {
                continue;
            };
            let Some((name, value)) = rest.split_once(": &str = \"") else {
                continue;
            };
            if name == "REASON" {
                continue;
            }
            let Some((literal, _)) = value.split_once('"') else {
                continue;
            };
            declared.push(literal.to_string());
        }
        declared.sort();
        let mut listed: Vec<String> = ALL.iter().map(|s| s.to_string()).collect();
        listed.sort();
        assert!(declared.len() > 40, "the declarations were read");
        assert_eq!(declared, listed, "every identifier constant is in ALL");
    }

    /// The registry document's identifier column and `ALL` are equal, in
    /// either direction, so neither can drift from the other.
    #[test]
    fn registry_document_and_constants_are_equal() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("docs/raft-error-registry.md");
        let text = std::fs::read_to_string(&path).unwrap();
        let mut documented = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if !line.starts_with('|') || !line.contains('.') {
                continue;
            }
            let first: String = line
                .trim_start_matches('|')
                .split('|')
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            if first.contains('.')
                && first
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '.' || c == '_')
            {
                documented.push(first);
            }
        }
        let mut documented = documented;
        documented.sort();
        documented.dedup();
        let mut constants: Vec<String> = ALL
            .iter()
            .map(|identifier| identifier.to_string())
            .collect();
        constants.sort();
        assert_eq!(documented, constants, "registry document and reasons.rs");
    }
}
