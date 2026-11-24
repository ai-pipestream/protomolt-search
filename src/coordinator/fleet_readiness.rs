//! What one `ClusterHealth` answer says about a fleet's readiness to serve.

use crate::pb::ClusterHealthResponse;

/// Primary-shard readiness read from one `ClusterHealth` answer, with the
/// reason for every obstacle that keeps the fleet from serving, split by
/// whether waiting can clear it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetReadiness {
    /// Primaries that answered their health probe.
    pub reachable: usize,
    /// Primaries the caller expects, or the number the coordinator lists
    /// when the caller expects no particular count.
    pub expected: usize,
    /// One line per primary that did not answer its health probe, with its
    /// address and probe error. A primary that is still opening its store
    /// is here until it answers, so these can clear on their own.
    pub unanswered: Vec<String>,
    /// One line per obstacle that no amount of waiting clears: a listing
    /// whose primary count is not the expected one, an empty listing, a
    /// primary that answers with an error of its own (it serves another
    /// collection), and primaries that do not score in one space. Nothing
    /// a starting fleet does changes these; each stands until the
    /// topology, the expectation, or a node's configuration is changed.
    pub misconfigured: Vec<String>,
}

impl FleetReadiness {
    /// Read `health` against `expect_shards` primaries; 0 expects whatever
    /// the coordinator lists.
    pub fn of(health: &ClusterHealthResponse, expect_shards: usize) -> Self {
        let primaries: Vec<_> = health.targets.iter().filter(|t| !t.is_replica).collect();
        let expected = if expect_shards > 0 {
            expect_shards
        } else {
            primaries.len()
        };
        let unanswered = primaries
            .iter()
            .filter(|t| !t.reachable)
            .map(|t| format!("shard {} at {} unreachable: {}", t.shard, t.addr, t.error))
            .collect();
        let mut misconfigured: Vec<String> = primaries
            .iter()
            .filter(|t| t.reachable && !t.error.is_empty())
            .map(|t| format!("shard {} at {} answers, but: {}", t.shard, t.addr, t.error))
            .collect();
        if primaries.len() != expected {
            misconfigured.push(format!(
                "coordinator lists {} primary shards, expected {expected}",
                primaries.len()
            ));
        } else if expected == 0 {
            misconfigured.push("coordinator lists no primary shards".to_string());
        }
        if !health.provider_mismatch.is_empty() {
            misconfigured.push(format!("provider mismatch: {}", health.provider_mismatch));
        }
        Self {
            reachable: primaries.iter().filter(|t| t.reachable).count(),
            expected,
            unanswered,
            misconfigured,
        }
    }

    /// True when the coordinator lists exactly the expected primaries,
    /// every one of them answered without an error of its own, and they
    /// score in one space.
    pub fn is_ready(&self) -> bool {
        self.unanswered.is_empty() && self.misconfigured.is_empty()
    }

    /// False when an obstacle stands that waiting cannot clear, so another
    /// look at this fleet as configured will find it not ready again.
    pub fn can_become_ready(&self) -> bool {
        self.misconfigured.is_empty()
    }

    /// Every obstacle, one line each: the primaries that did not answer,
    /// then what waiting cannot clear. Empty exactly when the fleet is
    /// ready.
    pub fn blockers(&self) -> impl Iterator<Item = &str> {
        self.unanswered
            .iter()
            .chain(&self.misconfigured)
            .map(String::as_str)
    }

    /// True when `error`, from connecting to the coordinator or from its
    /// `ClusterHealth` call, says only that the coordinator is not serving
    /// yet: the connection was refused, or the call came back
    /// `Unavailable`. Every other failure (credentials the coordinator
    /// refuses, a TLS handshake that fails, a malformed request) will
    /// fail the same way on the next attempt.
    pub fn coordinator_not_serving_yet(error: &(dyn std::error::Error + 'static)) -> bool {
        let mut cause = Some(error);
        while let Some(current) = cause {
            if let Some(status) = current.downcast_ref::<tonic::Status>() {
                return status.code() == tonic::Code::Unavailable;
            }
            if let Some(io) = current.downcast_ref::<std::io::Error>() {
                return io.kind() == std::io::ErrorKind::ConnectionRefused;
            }
            cause = current.source();
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::ShardHealth;

    fn target(shard: u32, is_replica: bool, reachable: bool) -> ShardHealth {
        ShardHealth {
            shard,
            addr: format!("10.0.0.{shard}:59300"),
            is_replica,
            reachable,
            error: if reachable {
                String::new()
            } else {
                "connection refused".to_string()
            },
            health: None,
        }
    }

    fn health(targets: Vec<ShardHealth>) -> ClusterHealthResponse {
        ClusterHealthResponse {
            targets,
            ..Default::default()
        }
    }

    #[test]
    fn ready_when_every_expected_primary_answers() {
        let answer = health(vec![target(0, false, true), target(1, false, true)]);
        for expect in [0, 2] {
            let state = FleetReadiness::of(&answer, expect);
            assert!(state.is_ready(), "expect {expect}: {state:?}");
            assert_eq!((state.reachable, state.expected), (2, 2));
        }
    }

    #[test]
    fn names_each_unreachable_primary_with_its_address_and_error() {
        let answer = health(vec![
            target(0, false, true),
            target(1, false, false),
            target(2, false, false),
        ]);
        let state = FleetReadiness::of(&answer, 3);
        assert!(!state.is_ready());
        assert_eq!((state.reachable, state.expected), (1, 3));
        assert_eq!(
            state.unanswered,
            [
                "shard 1 at 10.0.0.1:59300 unreachable: connection refused",
                "shard 2 at 10.0.0.2:59300 unreachable: connection refused",
            ]
        );
        assert!(
            state.can_become_ready(),
            "a primary that has not answered yet can still answer"
        );
    }

    #[test]
    fn replicas_neither_count_nor_block() {
        let answer = health(vec![
            target(0, false, true),
            target(0, true, false),
            target(1, true, true),
        ]);
        let state = FleetReadiness::of(&answer, 1);
        assert!(state.is_ready(), "{state:?}");
        assert_eq!(state.reachable, 1);
    }

    #[test]
    fn a_listing_with_fewer_primaries_than_expected_is_not_ready() {
        let answer = health(vec![target(0, false, true)]);
        let state = FleetReadiness::of(&answer, 2);
        assert_eq!(
            state.misconfigured,
            ["coordinator lists 1 primary shards, expected 2"]
        );
        assert!(!state.is_ready());
    }

    /// A coordinator's listing is its topology, which no node changes by
    /// finishing its startup: a count other than the expected one stands
    /// however long the caller waits, in either direction.
    #[test]
    fn a_listing_of_another_size_cannot_become_ready_by_waiting() {
        let listed_two = health(vec![target(0, false, true), target(1, false, true)]);
        for expect in [1, 3] {
            let state = FleetReadiness::of(&listed_two, expect);
            assert!(!state.can_become_ready(), "expect {expect}: {state:?}");
            assert!(state.unanswered.is_empty(), "{state:?}");
        }
        assert!(!FleetReadiness::of(&health(Vec::new()), 0).can_become_ready());
        assert!(!FleetReadiness::of(&health(Vec::new()), 4).can_become_ready());
    }

    /// The coordinator marks a primary that answers for another collection
    /// with an error of its own, while still reporting it reachable.
    #[test]
    fn a_primary_that_answers_with_an_error_cannot_become_ready_by_waiting() {
        let mut wrong_collection = target(1, false, true);
        wrong_collection.error =
            "node serves collection \"a\", but this coordinator is \"b\"".to_string();
        let answer = health(vec![target(0, false, true), wrong_collection]);
        let state = FleetReadiness::of(&answer, 2);
        assert_eq!((state.reachable, state.expected), (2, 2));
        assert_eq!(
            state.misconfigured,
            ["shard 1 at 10.0.0.1:59300 answers, but: node serves collection \"a\", but this coordinator is \"b\""]
        );
        assert!(!state.is_ready() && !state.can_become_ready());
    }

    /// The expected count answering is not enough when the coordinator
    /// also lists a primary that is down: the fleet has a hole.
    #[test]
    fn an_extra_unreachable_primary_is_not_ready() {
        let answer = health(vec![
            target(0, false, true),
            target(1, false, true),
            target(2, false, false),
        ]);
        let state = FleetReadiness::of(&answer, 2);
        assert_eq!((state.reachable, state.expected), (2, 2));
        assert_eq!(
            state.blockers().collect::<Vec<_>>(),
            [
                "shard 2 at 10.0.0.2:59300 unreachable: connection refused",
                "coordinator lists 3 primary shards, expected 2",
            ]
        );
        assert!(!state.can_become_ready());
    }

    /// Every primary answers, but they do not score in one space: the
    /// coordinator refuses Search and Query until that is repaired, and a
    /// node's backend and fingerprint are its configuration, not a phase
    /// of its startup.
    #[test]
    fn a_provider_mismatch_cannot_become_ready_by_waiting() {
        let mut answer = health(vec![target(0, false, true), target(1, false, true)]);
        answer.provider_mismatch = "shard 0 serves hnsw, shard 1 serves flat".to_string();
        let state = FleetReadiness::of(&answer, 2);
        assert_eq!((state.reachable, state.expected), (2, 2));
        assert_eq!(
            state.misconfigured,
            ["provider mismatch: shard 0 serves hnsw, shard 1 serves flat"]
        );
        assert!(!state.is_ready() && !state.can_become_ready());
    }

    /// A failure with `cause` underneath, the way a transport error
    /// carries the socket error.
    #[derive(Debug)]
    struct Wrapped(Box<dyn std::error::Error + 'static>);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("transport error")
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    #[test]
    fn only_a_refused_connection_or_unavailable_call_is_worth_another_attempt() {
        use std::io::{Error as Io, ErrorKind};
        let socket = |kind| Wrapped(Box::new(Wrapped(Box::new(Io::new(kind, "socket")))));
        let not_yet = FleetReadiness::coordinator_not_serving_yet;
        assert!(not_yet(&socket(ErrorKind::ConnectionRefused)));
        assert!(not_yet(&tonic::Status::unavailable("starting")));
        assert!(!not_yet(&socket(ErrorKind::InvalidData)), "a TLS failure");
        assert!(!not_yet(&tonic::Status::unauthenticated("bad token")));
        assert!(!not_yet(&tonic::Status::permission_denied("no grant")));
        assert!(!not_yet(&Wrapped(Box::new(std::fmt::Error))));
    }

    #[test]
    fn an_empty_listing_is_never_ready() {
        let state = FleetReadiness::of(&health(Vec::new()), 0);
        assert_eq!(state.misconfigured, ["coordinator lists no primary shards"]);
        assert!(!FleetReadiness::of(&health(Vec::new()), 4).is_ready());
    }
}
