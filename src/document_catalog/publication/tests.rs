use super::*;
use crate::{
    analyzer::body_spec,
    mapping,
    node::{NodeConfig, NodeServiceImpl},
    pb::*,
    segments::{SegmentRowRetirement, SegmentSource},
};
use crate::{
    pb::storage::{
        DocumentCatalogCheckpointLimits, SourceBackupLimits, SourceBackupManifest,
        SourceRestoreRequest,
    },
    stats_identity::StatsClaim,
};
use std::{path::PathBuf, sync::Arc};

mod fixture;
use fixture::*;

mod backup;
mod backup_audit;
mod backup_retained;
mod checkpoint;
mod cutover;
mod journal;
mod maintenance;
mod maintenance_checkpoint;
mod restore;
mod restore_damage;
mod retirement;
mod retirement_backup;
mod seal;
