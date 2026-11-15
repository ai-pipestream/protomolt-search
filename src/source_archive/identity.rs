//! Identity snapshots that outlive the archive they were captured from.

use super::*;

/// Immutable identity metadata for the rows visible when it was captured.
/// It owns no original payload bytes and remains usable after the archive
/// is changed, dropped, or replaced by compaction.
#[derive(Clone, Debug, Default)]
pub struct IdentitySnapshot {
    pub(super) data: IdentitySnapshotData,
}

#[derive(Clone, Debug, Default)]
pub(super) enum IdentitySnapshotData {
    #[default]
    Empty,
    Building {
        rows: SharedPages<SourceRow>,
        identities: SharedPages<SharedIdentity>,
    },
    Encoded(Arc<SourceArchiveIndex>),
    Parts(Arc<[IdentityPart]>),
}

#[derive(Clone, Debug)]
pub(super) struct IdentityPart {
    pub(super) base: u32,
    pub(super) rows: u32,
    pub(super) snapshot: IdentitySnapshot,
}

impl IdentitySnapshot {
    /// Compose already captured parts in shard-local row order. Holes are
    /// permitted, but overlap and overflow would alias another row's identity.
    pub(crate) fn from_parts(
        parts: impl IntoIterator<Item = (u32, u32, Self)>,
    ) -> Result<Self, String> {
        let mut captured = Vec::new();
        let mut previous_end = 0u64;
        for (base, rows, snapshot) in parts {
            if rows == 0 {
                continue;
            }
            let end = u64::from(base) + u64::from(rows);
            if u64::from(base) < previous_end || end > u64::from(u32::MAX) + 1 {
                return Err(
                    "identity snapshot parts overlap, are out of order, or overflow local rows"
                        .into(),
                );
            }
            previous_end = end;
            captured.push(IdentityPart {
                base,
                rows,
                snapshot,
            });
        }
        Ok(Self {
            data: IdentitySnapshotData::Parts(captured.into()),
        })
    }

    /// Resolve the original binding, independently of later mutation. This
    /// view contains neither a live-row filter nor an authorization decision.
    pub fn identity(&self, row: u32) -> Option<DocumentIdentity> {
        match &self.data {
            IdentitySnapshotData::Empty => None,
            IdentitySnapshotData::Parts(parts) => {
                let part = &parts[parts
                    .partition_point(|part| part.base <= row)
                    .checked_sub(1)?];
                let local = row - part.base;
                if local >= part.rows {
                    return None;
                }
                part.snapshot.identity(local)
            }
            IdentitySnapshotData::Building { rows, identities } => {
                let row = rows.get(row as usize)?;
                let identity = identities.get(row.identity.checked_sub(1)? as usize)?;
                Some(DocumentIdentity {
                    document_key: identity.document_key.to_vec(),
                    version: identity.version,
                    chunk_ordinal: row.chunk_ordinal,
                })
            }
            IdentitySnapshotData::Encoded(index) => {
                row_identity(&index.rows, &index.identities, row)
            }
        }
    }
}
