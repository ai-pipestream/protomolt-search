//! FP32 scoring and row reads.

use super::*;

impl ExactVectorStore {
    /// Score local slots by FP32 dot product, returning `(slot, score)` in
    /// request order. Callers own global-id routing and final ordering.
    pub fn score_slots(&self, query: &[f32], slots: &[usize]) -> io::Result<Vec<(usize, f32)>> {
        self.score_slots_profiled(query, slots, 1)
            .map(|result| result.rows)
    }

    /// The store's dimension, which `query` must match with finite values.
    fn check_query(&self, query: &[f32]) -> io::Result<usize> {
        let dim = self
            .dim()
            .ok_or_else(|| invalid("exact-vector store has no dimension"))?;
        if query.len() != dim {
            return Err(invalid(format!(
                "query dim {} does not match exact-vector dim {dim}",
                query.len()
            )));
        }
        if let Some((coordinate, value)) = query
            .iter()
            .copied()
            .enumerate()
            .find(|(_, v)| !v.is_finite())
        {
            return Err(invalid(format!(
                "query coordinate {coordinate} is not finite: {value}"
            )));
        }
        Ok(dim)
    }

    /// Score `(ordinal, slot)` pairs in the given order on up to `tasks`
    /// scoped workers, keeping the order.
    fn score_scheduled(
        &self,
        query: &[f32],
        scheduled: &[(usize, usize)],
        dim: usize,
        tasks: usize,
    ) -> io::Result<Vec<(usize, usize, f32)>> {
        let score = |part: &[(usize, usize)]| {
            part.iter()
                .map(|&(ordinal, slot)| {
                    self.score_one(query, slot, dim)
                        .map(|score| (ordinal, slot, score))
                })
                .collect::<io::Result<Vec<_>>>()
        };
        if tasks <= 1 {
            return score(scheduled);
        }
        let chunk = scheduled.len().div_ceil(tasks);
        std::thread::scope(|scope| -> io::Result<Vec<_>> {
            let handles: Vec<_> = scheduled
                .chunks(chunk)
                .map(|part| scope.spawn(move || score(part)))
                .collect();
            let mut all = Vec::with_capacity(scheduled.len());
            for handle in handles {
                all.extend(
                    handle
                        .join()
                        .map_err(|_| io::Error::other("exact rerank worker panicked"))??,
                );
            }
            Ok(all)
        })
    }

    /// Score local slots after ordering reads by mmap page, using at most
    /// `parallelism` bounded worker lanes. Every individual dot product keeps
    /// the original scalar accumulation order, so scheduling does not change
    /// score bits. The result is restored to request order before return.
    pub fn score_slots_profiled(
        &self,
        query: &[f32],
        slots: &[usize],
        parallelism: usize,
    ) -> io::Result<ScoredSlots> {
        let dim = self.check_query(query)?;
        let row_bytes = dim
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| invalid("exact-vector row size overflow"))?;
        let mut scheduled: Vec<(usize, usize)> = slots
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(ordinal, slot)| self.contains_row(slot).then_some((ordinal, slot)))
            .collect();
        scheduled.sort_unstable_by_key(|&(ordinal, slot)| {
            let byte = HEADER_BYTES.saturating_add(slot.saturating_mul(row_bytes));
            (byte / PAGE_BYTES, slot, ordinal)
        });
        let tasks = rerank_task_count(scheduled.len(), parallelism);
        let mut restored = self.score_scheduled(query, &scheduled, dim, tasks)?;
        restored.sort_unstable_by_key(|&(ordinal, _, _)| ordinal);
        let rows = restored
            .into_iter()
            .map(|(_, slot, score)| (slot, score))
            .collect::<Vec<_>>();
        let mut pages = std::collections::BTreeSet::new();
        for &(_, slot) in &scheduled {
            self.mapped_pages(slot, row_bytes, &mut pages);
        }
        let pages_touched = pages.len() as u64;
        let logical_bytes = u64::try_from(rows.len())
            .ok()
            .and_then(|count| count.checked_mul(row_bytes as u64))
            .ok_or_else(|| invalid("exact-vector logical byte count overflow"))?;
        Ok(ScoredSlots {
            rows,
            logical_bytes,
            pages_touched,
            tasks: tasks as u32,
        })
    }

    /// Whether a physical row has an FP32 vector. This is storage presence,
    /// not a live-document or authorization decision.
    pub fn contains_row(&self, slot: usize) -> bool {
        match &self.storage {
            Storage::Segmented(view) => view.contains_row(slot),
            Storage::Appended { base, delta } => {
                if slot < base.len() {
                    base.contains_row(slot)
                } else {
                    delta.contains_row(slot - base.len())
                }
            }
            _ => slot < self.len(),
        }
    }

    // Mapped page identities include the held mapping, not just its file offset.
    pub(super) fn mapped_pages(
        &self,
        slot: usize,
        row_bytes: usize,
        pages: &mut std::collections::BTreeSet<(usize, usize)>,
    ) {
        match &self.storage {
            Storage::Mapped { map, .. } => {
                let start = HEADER_BYTES + slot * row_bytes;
                let end = start + row_bytes - 1;
                pages.extend(
                    (start / PAGE_BYTES..=end / PAGE_BYTES)
                        .map(|page| (map.as_ptr() as usize, page)),
                );
            }
            Storage::Segmented(view) => view.mapped_pages(slot, row_bytes, pages),
            Storage::Appended { base, .. } if slot < base.len() => {
                base.mapped_pages(slot, row_bytes, pages)
            }
            _ => {}
        }
    }

    pub(super) fn score_one(&self, query: &[f32], slot: usize, dim: usize) -> io::Result<f32> {
        Ok(match &self.storage {
            Storage::Building { values, .. } => dot(&values[slot * dim..(slot + 1) * dim], query),
            Storage::Segmented(view) => view.score_one(query, slot, dim)?,
            Storage::Spilled { file, .. } => {
                let mut row = vec![0u8; dim * 4];
                file.read_exact_at(&mut row, (HEADER_BYTES + slot * dim * 4) as u64)?;
                dot_bytes(&row, query)
            }
            Storage::Mapped { map, .. } => dot_mapped(
                &map[HEADER_BYTES + slot * dim * 4..HEADER_BYTES + (slot + 1) * dim * 4],
                query,
            ),
            Storage::Appended { base, delta } => {
                if slot < base.len() {
                    base.score_one(query, slot, dim)?
                } else {
                    delta.score_one(query, slot - base.len(), dim)?
                }
            }
        })
    }

    /// Read all FP32 rows in [from, to) for sealing or reconstruction. Invalid
    /// ranges and short reads refuse the entire result instead of returning a
    /// partial range or synthesizing vector coordinates.
    pub fn row_values(&self, from: usize, to: usize) -> io::Result<Vec<f32>> {
        if from > to || to > self.len() {
            return Err(invalid("exact-vector row range is out of bounds"));
        }
        if from == to {
            return Ok(Vec::new());
        }
        let dim = self
            .dim()
            .ok_or_else(|| invalid("exact-vector store has no dimension"))?;
        Ok(match &self.storage {
            Storage::Segmented(view) => view.row_values(from, to)?,
            Storage::Building { values, .. } => values[from * dim..to * dim].to_vec(),
            Storage::Spilled { file, .. } => {
                let mut bytes = vec![0u8; (to - from) * dim * 4];
                file.read_exact_at(&mut bytes, (HEADER_BYTES + from * dim * 4) as u64)?;
                bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b))
                    .collect()
            }
            Storage::Mapped { map, .. } => map
                [HEADER_BYTES + from * dim * 4..HEADER_BYTES + to * dim * 4]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f32::from_le_bytes(*bytes))
                .collect(),
            Storage::Appended { base, delta } => {
                let boundary = base.len();
                if to <= boundary {
                    base.row_values(from, to)?
                } else if from >= boundary {
                    delta.row_values(from - boundary, to - boundary)?
                } else {
                    let mut result = base.row_values(from, boundary)?;
                    result.extend(delta.row_values(0, to - boundary)?);
                    result
                }
            }
        })
    }
}
