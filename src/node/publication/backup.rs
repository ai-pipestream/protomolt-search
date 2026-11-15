//! Source-authority backup admission over a segmented node's live catalog.

use super::*;
use crate::document_catalog::DocumentCatalog;
use crate::stats_identity::StatsClaim;

impl NodeServiceImpl {
    /// The live catalog and its shared publication fence, admitted for the
    /// trusted backup owner. Never substitute a second catalog opened by path.
    pub(crate) fn document_backup_catalog(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
    ) -> Result<crate::segments::SegmentCatalog, Status> {
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "backup source and node collections differ",
            ));
        }
        let owner = source.index_owner(index_key)?;
        let guard = read_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "source backup needs a segmented node",
            ));
        };
        let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
        self.check_segment_publication(&guard, claim, shard.snapshot().epoch())?;
        check_source_live_view(shard.snapshot(), &guard.live_docs)?;
        if shard
            .snapshot()
            .manifest()
            .source_owner
            .as_ref()
            .map(|o| o.decode())
            .transpose()
            .map_err(Status::data_loss)?
            .is_some_and(|o| o != owner)
        {
            return Err(Status::failed_precondition(
                "backup node belongs to another source owner",
            ));
        }
        Ok(shard.catalog().clone())
    }

    /// Back up a source authority with exactly one journaled local index. The
    /// multi-index owner uses `DocumentCatalog::capture_backup` with every live
    /// catalog. This is synchronous local storage work, not an administration
    /// RPC, a serving activation, or permission to export a phone's shard.
    pub fn backup_documents_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
        destination: &Path,
        limits: &crate::pb::storage::SourceBackupLimits,
    ) -> Result<crate::pb::storage::SourceBackupManifest, Status> {
        let catalog = self.document_backup_catalog(source, index_key)?;
        source
            .capture_backup(&[(index_key, &catalog)], limits)?
            .write_to(destination)
    }
}
