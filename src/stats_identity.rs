//! Statistics are identified by an opaque shard lifetime and its mutation epoch.
//! Neither a network address nor a process-local counter identifies a lifetime.
use std::sync::OnceLock;
use tonic::Status;

/// One shard's fenced statistics version. The default means no claim.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsClaim {
    pub epoch: u64,
    incarnation: Option<[u8; 32]>,
}

impl StatsClaim {
    pub fn from_wire(epoch: u64, incarnation: &[u8]) -> Result<Self, Status> {
        if epoch == 0 && incarnation.is_empty() {
            return Ok(Self::default());
        }
        let incarnation = <[u8; 32]>::try_from(incarnation)
            .map_err(|_| stale("a nonzero statistics claim requires a 32-byte incarnation"))?;
        if epoch == 0 {
            return Err(stale("a statistics incarnation requires a nonzero epoch"));
        }
        Ok(Self {
            epoch,
            incarnation: Some(incarnation),
        })
    }

    /// Responses used by a cache or planner must supply a complete version.
    pub fn required(epoch: u64, incarnation: &[u8]) -> Result<Self, Status> {
        let claim = Self::from_wire(epoch, incarnation)?;
        if claim.epoch == 0 {
            return Err(stale("statistics response omitted its version"));
        }
        Ok(claim)
    }

    pub fn incarnation(self) -> Vec<u8> {
        self.incarnation
            .map_or_else(Vec::new, |value| value.to_vec())
    }
}

/// Generated once per in-memory shard or relay lifetime. Entropy failure is
/// retained and returned as an error, never replaced by a clock or counter.
#[derive(Default)]
pub(crate) struct StatsIncarnation(OnceLock<Result<[u8; 32], String>>);

impl StatsIncarnation {
    pub(crate) fn bytes(&self) -> Result<Vec<u8>, Status> {
        match self.0.get_or_init(|| {
            let mut bytes = [0; 32];
            getrandom::getrandom(&mut bytes).map_err(|e| e.to_string())?;
            Ok(bytes)
        }) {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(error) => Err(Status::internal(format!(
                "statistics incarnation entropy: {error}"
            ))),
        }
    }

    pub(crate) fn check(&self, epoch: u64, expected: u64, identity: &[u8]) -> Result<(), Status> {
        let claim = StatsClaim::from_wire(expected, identity)?;
        if claim == StatsClaim::default() {
            return Ok(());
        }
        if expected != epoch || identity != self.bytes()? {
            return Err(stale(&format!(
                "request epoch {expected}, current epoch {epoch}; statistics lifetime or postings changed; refetch TermStats"
            )));
        }
        Ok(())
    }
}

fn stale(message: &str) -> Status {
    Status::failed_precondition(format!("{}: {message}", crate::node::STALE_STATS_EPOCH))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_claims_and_legacy_responses_are_rejected() {
        assert_eq!(
            StatsClaim::from_wire(0, &[]).unwrap(),
            StatsClaim::default()
        );
        assert!(StatsClaim::required(0, &[]).is_err());
        for length in [0, 1, 31, 33] {
            assert!(StatsClaim::required(7, &vec![1; length]).is_err());
        }
        assert!(StatsClaim::from_wire(0, &[1; 32]).is_err());
    }

    #[test]
    fn same_epoch_in_another_lifetime_never_satisfies_the_claim() {
        let first = StatsIncarnation::default();
        let next = StatsIncarnation::default();
        let identity = first.bytes().unwrap();
        assert_eq!(identity, first.bytes().unwrap());
        first.check(4, 4, &identity).unwrap();
        assert!(next.check(4, 4, &identity).is_err());
        assert!(first.check(5, 4, &identity).is_err());
        first.check(4, 0, &[]).unwrap();
    }

    #[test]
    fn entropy_failure_is_not_replaced_by_a_reusable_identity() {
        let identity = StatsIncarnation::default();
        identity
            .0
            .set(Err("injected entropy failure".into()))
            .unwrap();
        for _ in 0..2 {
            let error = identity.bytes().unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert!(error.message().contains("injected entropy failure"));
        }
    }
}
