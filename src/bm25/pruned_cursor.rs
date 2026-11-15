//! Ordered impact cursors and conservative per-field bounds.

use super::{block_max, frontier_max, idf, tf_norm, Bm25Params, FieldQuery, PruneStats};

pub(super) struct Pair<'a> {
    pub order: usize,
    pub field: usize,
    pub scale: f64,
    pub params: Bm25Params,
    pub avgdl: f64,
    pub cursor: crate::postings::ImpactCursor<'a>,
    pub block_max: f64,
    pub block: u32,
    pub l1_max: f64,
    pub l1_group: u32,
    pub term_max: f64,
}

impl Pair<'_> {
    pub fn refresh(&mut self) {
        if self.cursor.block() == self.block {
            return;
        }
        self.block = self.cursor.block();
        self.block_max = block_max(&self.cursor, self.scale, self.params, self.avgdl);
        if self.cursor.l1_group() != self.l1_group {
            self.l1_group = self.cursor.l1_group();
            self.l1_max = frontier_max(
                self.cursor.l1_frontier(),
                self.scale,
                self.params,
                self.avgdl,
            );
        }
    }

    pub fn contribution(&self, lengths: &[u32]) -> f64 {
        self.scale
            * tf_norm(
                self.params,
                self.cursor.tf(),
                lengths[self.field],
                self.avgdl,
            )
    }
}

pub(super) fn open<'a>(fields: &[FieldQuery<'a>], prune: &mut PruneStats) -> Option<Vec<Pair<'a>>> {
    let count: usize = fields.iter().map(|field| field.terms.len()).sum();
    if count > 128
        || fields
            .iter()
            .any(|field| field.weight < 0.0 || field.weight.is_nan())
    {
        return None;
    }
    let mut pairs = Vec::new();
    let mut order = 0;
    for (field, query) in fields.iter().enumerate() {
        debug_assert_eq!(query.terms.len(), query.stats.dfs.len());
        for (term, text) in query.terms.iter().enumerate() {
            let position = order;
            order += 1;
            if query.stats.dfs[term] == 0 || query.index.df(text) == 0 {
                continue;
            }
            let cursor = query.index.impacts(text)?;
            let avgdl = query.stats.avgdl();
            let scale = query.weight * idf(query.stats.doc_count, query.stats.dfs[term]);
            let l1_max = frontier_max(cursor.l1_frontier(), scale, query.params, avgdl);
            let term_max = frontier_max(cursor.term_frontier(), scale, query.params, avgdl);
            prune.blocks_total += u64::from(cursor.n_blocks());
            pairs.push(Pair {
                order: position,
                field,
                scale,
                params: query.params,
                avgdl,
                block: cursor.block(),
                block_max: block_max(&cursor, scale, query.params, avgdl),
                l1_group: cursor.l1_group(),
                l1_max,
                term_max,
                cursor,
            });
        }
    }
    Some(pairs)
}
