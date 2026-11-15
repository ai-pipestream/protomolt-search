//! Dictionary prefix scans under a visibility scope.

use crate::node::*;

impl NodeServiceImpl {
    /// Resolve the mandatory view and scan under the same shard read guard.
    /// Scoped counts describe live admitted documents, never physical rows.
    pub(in crate::node) fn scan_dictionary(
        &self,
        field: &str,
        prefix: &str,
        cap: usize,
        visibility: Option<&crate::pb::DocumentVisibility>,
    ) -> Result<crate::pb::SuggestTermsResponse, Status> {
        if prefix.is_empty() {
            return Err(Status::invalid_argument("a term prefix must be non-empty"));
        }
        let scope = crate::visibility::VisibilityScope::new(visibility)?;
        let filter = visibility.and_then(|view| view.filter.as_ref());
        let guard = read_shard(&self.state);
        if guard
            .bm25
            .as_ref()
            .is_some_and(|store| store.as_index().is_none())
        {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let (_, columns_known) = filter_known_flags(guard.bm25.as_ref(), &[], filter);
        let mut response = crate::pb::SuggestTermsResponse {
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: columns_known,
            tombstoned_rows: if visibility.is_some() {
                0
            } else {
                guard.live_docs.deleted_count()
            },
            ..Default::default()
        };
        let Some(store) = guard.bm25.as_ref() else {
            return Ok(response);
        };
        let Some(fi) = store.field_index(field) else {
            return Ok(response);
        };
        let index = store.field_view(fi).expect("searchability checked above");
        let entries = if visibility.is_some() {
            let (_, allow, _) = resolve_shard_filters(
                Some(store),
                None,
                store.next_doc_id() as usize,
                &[],
                &[],
                filter,
                self.knobs.segment_pruning(),
                None,
            )?;
            let admitted = |doc: u32| {
                !guard.live_docs.is_deleted(doc as usize)
                    && allow.as_ref().is_some_and(|mask| mask.test(doc as usize))
            };
            index.suggest_visible_prefix(prefix, cap, &admitted)
        } else {
            index.suggest_prefix(prefix, cap)
        };
        response.known = true;
        match entries {
            Ok(entries) => {
                response.count = entries.len() as u64;
                response.entries = entries
                    .into_iter()
                    .map(|(term, df)| crate::pb::SuggestTermEntry {
                        term,
                        df: u64::from(df),
                    })
                    .collect();
            }
            Err(count) => response.count = count as u64,
        }
        Ok(response)
    }
}
