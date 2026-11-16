//! BM25 and vector search over one segment-set snapshot under a global order.

use super::*;

struct NoColumns;

impl crate::scorefn::NumericRead for NoColumns {
    fn uint_value(&self, _ii: usize, _doc_id: u32) -> Option<u64> {
        None
    }
    fn value(&self, _ni: usize, _doc_id: u32) -> Option<f64> {
        None
    }
    fn map_value(&self, _column: usize, _key_ord: u32, _doc_id: u32) -> Option<f64> {
        None
    }
    fn map_int_value(&self, _: usize, _: u32, _: u32) -> Option<i64> {
        None
    }
    fn map_uint_value(&self, _: usize, _: u32, _: u32) -> Option<u64> {
        None
    }
    fn int_value(&self, _ii: usize, _doc_id: u32) -> Option<i64> {
        None
    }
    fn geo_value(&self, _gi: usize, _doc_id: u32) -> Option<(f64, f64)> {
        None
    }
    fn facet_ord(&self, _fi: usize, _doc_id: u32) -> Option<u32> {
        None
    }
    fn map_facet_value_ord(&self, _ci: usize, _key_ord: u32, _doc_id: u32) -> Option<u32> {
        None
    }
}

impl OpenedSegmentSet {
    pub fn global_bm25_stats(&self, terms: &[String]) -> Result<CorpusStats, String> {
        let mut stats = CorpusStats {
            doc_count: 0,
            total_doc_length: 0,
            dfs: vec![0; terms.len()],
        };
        for segment in &self.segments {
            let (document_count, total_document_length, dfs) =
                live_stats_share(segment.bm25.as_ref(), &segment.live_docs, terms)?;
            stats.doc_count = stats
                .doc_count
                .checked_add(document_count)
                .ok_or("global BM25 document count overflow")?;
            stats.total_doc_length = stats
                .total_doc_length
                .checked_add(total_document_length)
                .ok_or("global BM25 length overflow")?;
            for (df, share) in stats.dfs.iter_mut().zip(dfs) {
                *df = df
                    .checked_add(share)
                    .ok_or("global BM25 document frequency overflow")?;
            }
        }
        Ok(stats)
    }

    pub fn search_bm25(
        &self,
        terms: &[String],
        params: Bm25Params,
        k: usize,
    ) -> Result<Vec<SegmentHit>, String> {
        if k == 0 || terms.is_empty() {
            return Ok(Vec::new());
        }
        let stats = self.global_bm25_stats(terms)?;
        let columns = NoColumns;
        let mut all = Vec::new();
        for segment in &self.segments {
            let filter = crate::filter::DocFilter {
                deleted: segment.live_docs.words(),
                ..Default::default()
            };
            let mut pruning = bm25::PruneStats::default();
            let hits = bm25::top_k(
                segment.bm25.as_ref(),
                terms,
                &stats,
                params,
                bm25::TopK::new(k)
                    .filter(Some((&filter, &columns)))
                    .prune_stats(&mut pruning),
            );
            for hit in hits {
                all.push(SegmentHit {
                    doc_id: segment
                        .metadata
                        .base_label
                        .checked_add(u64::from(hit.doc_id))
                        .ok_or("segment BM25 id overflow")?,
                    score: hit.score as f32,
                    segment_id: segment.metadata.segment_id.clone(),
                });
            }
        }
        sort_hits(&mut all, k);
        Ok(all)
    }

    pub fn search_vector(&self, vector: &[f32], k: usize) -> Result<Vec<SegmentHit>, String> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut all = Vec::new();
        for segment in &self.segments {
            let Some(image) = segment.vector.as_ref() else {
                continue;
            };
            let rows = image.len();
            if rows == 0 {
                continue;
            }
            let allow: Vec<bool> = (0..rows)
                .map(|row| !segment.live_docs.is_deleted(row))
                .collect();
            let result = image
                .try_search(
                    vector,
                    k.min(rows),
                    VectorSearchOptions::new().with_allowlist(&allow),
                )
                .map_err(|error| {
                    format!(
                        "search vector segment {:?}: {error}",
                        segment.metadata.segment_id
                    )
                })?;
            if result.query_count != 1 {
                return Err(format!(
                    "vector segment {:?} returned {} result groups for one query",
                    segment.metadata.segment_id, result.query_count
                ));
            }
            for (&slot, &score) in result
                .slots_for_query(0)
                .iter()
                .zip(result.scores_for_query(0))
            {
                if slot < 0 || !score.is_finite() {
                    continue;
                }
                let slot = u64::try_from(slot).map_err(|_| "negative vector slot")?;
                all.push(SegmentHit {
                    doc_id: segment
                        .metadata
                        .base_label
                        .checked_add(slot)
                        .ok_or("segment vector id overflow")?,
                    score,
                    segment_id: segment.metadata.segment_id.clone(),
                });
            }
        }
        sort_hits(&mut all, k);
        Ok(all)
    }
}

fn live_stats_share(
    index: &dyn Bm25Index,
    live_docs: &LiveDocs,
    terms: &[String],
) -> Result<(u64, u64, Vec<u32>), String> {
    if !live_docs.has_deletes() {
        return Ok((
            index.doc_count(),
            index.total_doc_length(),
            terms.iter().map(|term| index.df(term)).collect(),
        ));
    }
    let rows = u32::try_from(live_docs.persisted_rows())
        .map_err(|_| "segment BM25 row count exceeds u32")?;
    let mut deleted_documents = 0u64;
    let mut deleted_length = 0u64;
    for slot in 0..rows {
        if live_docs.is_deleted(slot as usize) && index.text(slot).is_some() {
            deleted_documents = deleted_documents
                .checked_add(1)
                .ok_or("live BM25 document count overflow")?;
            deleted_length = deleted_length
                .checked_add(u64::from(index.doc_length(slot)))
                .ok_or("live BM25 length overflow")?;
        }
    }
    let mut dfs = Vec::with_capacity(terms.len());
    for term in terms {
        let mut live_df = 0u64;
        index.for_each_doc_tf(term, &mut |doc, _| {
            if !live_docs.is_deleted(doc as usize) {
                live_df += 1;
            }
        });
        dfs.push(u32::try_from(live_df).map_err(|_| "live BM25 document frequency exceeds u32")?);
    }
    Ok((
        index
            .doc_count()
            .checked_sub(deleted_documents)
            .ok_or("deleted BM25 document count exceeds total")?,
        index
            .total_doc_length()
            .checked_sub(deleted_length)
            .ok_or("deleted BM25 length exceeds total")?,
        dfs,
    ))
}

fn sort_hits(hits: &mut Vec<SegmentHit>, k: usize) {
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    hits.truncate(k);
}
