//! Activate only snapshots belonging to the current durable catalog.

use super::*;

impl SegmentedShard {
    /// Adopt the catalog snapshot that now contains the frozen part as a
    /// sealed segment. The tail stays as it is: the set's rows must end
    /// where the tail begins, or the shard refuses the snapshot and
    /// keeps serving what it had.
    pub fn republish(&mut self, set: Arc<OpenedSegmentSet>) -> Result<(), String> {
        let catalog = self.catalog.clone();
        let candidate = Arc::clone(&set);
        catalog.with_current_snapshot(&candidate, || self.adopt_published(set))
    }

    pub(crate) fn republish_cutover(
        &mut self,
        set: Arc<OpenedSegmentSet>,
        cutover: &crate::segments::CatalogCutover<'_>,
    ) -> Result<(), String> {
        let catalog = self.catalog.clone();
        let candidate = Arc::clone(&set);
        cutover.with_snapshot(&catalog, &candidate, || self.adopt_published(set))
    }

    fn adopt_published(&mut self, set: Arc<OpenedSegmentSet>) -> Result<(), String> {
        let expected = self.tail_base;
        let previous = (Arc::clone(&self.set), self.frozen.take());
        self.set = set;
        let outcome = match self.rebuild() {
            Ok(()) if self.tail_base == expected => Ok(()),
            Ok(()) => Err(format!(
                "the published set covers {} rows but the tail starts at {expected}; a \
                 republish must seal exactly the frozen rows",
                self.tail_base
            )),
            Err(error) => Err(error),
        };
        if outcome.is_err() {
            (self.set, self.frozen) = previous;
            self.rebuild()
                .expect("the previous snapshot rebuilt before this republish");
        }
        outcome
    }
}
