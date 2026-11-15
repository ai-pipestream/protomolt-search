//! Partition observations and the store that admits them cohort by cohort.

mod committed;
mod identity;
mod ingest;
mod store;

pub(super) use self::committed::check_observation_identity;
pub use self::committed::{CommittedCopy, CommittedShard, CommittedView, LeafCoverage};
pub use self::identity::{
    ObsKey, PartitionIdentity, PartitionObservation, ReporterIdentity, ShardRef,
};
pub use self::store::{observation_set_digest, IngestOutcome, ObservationStore};
