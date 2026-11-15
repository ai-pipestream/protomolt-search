//! Structural locality measurements of a linked layer-0 graph.

use super::super::graph::{LinkCells, View};

/// Structural locality summary of a linked layer-0 HNSW graph.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LocalityMetrics {
    /// Total directed layer-0 edges `(u -> v)`.
    pub edges: u64,
    /// Mean `|u - v|` across all directed layer-0 edges.
    pub mean_id_gap: f64,
    /// Median `|u - v|` across all directed layer-0 edges.
    pub median_id_gap: u32,
    /// Fraction of directed layer-0 edges whose endpoints start on the same
    /// 4 KB vector-row page (`(u * dim * 4) / 4096 == (v * dim * 4) / 4096`).
    pub same_4k_row_page_frac: f64,
    /// Fraction of directed layer-0 edges whose endpoints share the same
    /// 64-byte `inv_norm` cache line (`u / 16 == v / 16`).
    pub same_cacheline_inv_norm_frac: f64,
}

const OS_PAGE_BYTES: usize = 4_096;

/// Compute [`LocalityMetrics`] over `view`'s layer-0 adjacency.
pub(in crate::hnsw) fn compute_locality_metrics<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
) -> LocalityMetrics {
    let n = view.n();
    if n == 0 || !view.is_linked() {
        return LocalityMetrics::default();
    }
    let row_bytes = view.dim * std::mem::size_of::<f32>();
    let stride0 = view.m0 + 1;
    let mut gaps: Vec<u32> = Vec::with_capacity(n * view.m0);
    let mut sum_gap = 0u64;
    let mut same_page = 0u64;
    let mut same_line = 0u64;
    for u in 0..n {
        let start = u * stride0;
        let count = (LinkCells::get(view.links0, start) as usize).min(view.m0);
        let u_page = (u * row_bytes) / OS_PAGE_BYTES;
        let u_line = u / 16;
        for i in 0..count {
            let v = LinkCells::get(view.links0, start + 1 + i) as usize;
            let gap = u.abs_diff(v) as u32;
            gaps.push(gap);
            sum_gap += u64::from(gap);
            if (v * row_bytes) / OS_PAGE_BYTES == u_page {
                same_page += 1;
            }
            if v / 16 == u_line {
                same_line += 1;
            }
        }
    }
    if gaps.is_empty() {
        return LocalityMetrics::default();
    }
    gaps.sort_unstable();
    let edges = gaps.len() as u64;
    LocalityMetrics {
        edges,
        mean_id_gap: sum_gap as f64 / edges as f64,
        median_id_gap: gaps[gaps.len() / 2],
        same_4k_row_page_frac: same_page as f64 / edges as f64,
        same_cacheline_inv_norm_frac: same_line as f64 / edges as f64,
    }
}

/// Count distinct 4 KB vector-row pages touched during a layer-0 walk at `ef`.
pub(in crate::hnsw) fn walk_row_4k_pages<L: LinkCells + ?Sized>(
    view: &View<'_, L>,
    query: &[f32],
    ef: usize,
) -> usize {
    use super::super::graph::{with_scratch, Filter, FloorPrune, Hook, Patience};

    if view.n() == 0 || !view.is_linked() {
        return 0;
    }
    let row_bytes = view.dim * std::mem::size_of::<f32>();
    let total_pages = (view.n() * row_bytes).div_ceil(OS_PAGE_BYTES);
    struct PageHook {
        row_bytes: usize,
        touched: Vec<u64>,
    }
    impl Hook for PageHook {
        #[inline]
        fn scored(&mut self, node: u32, _score: f32, _row: &[f32]) {
            let start_byte = node as usize * self.row_bytes;
            let end_byte = start_byte + self.row_bytes.saturating_sub(1);
            for p in (start_byte / OS_PAGE_BYTES)..=(end_byte / OS_PAGE_BYTES) {
                self.touched[p / 64] |= 1u64 << (p % 64);
            }
        }
        #[inline]
        fn hop(&mut self) -> bool {
            true
        }
    }
    let mut hook = PageHook {
        row_bytes,
        touched: vec![0u64; total_pages.div_ceil(64)],
    };
    with_scratch(|scratch| {
        let _ = view.walk(
            query,
            ef.max(1),
            Filter::None,
            Patience::OFF,
            FloorPrune::OFF,
            scratch,
            &mut hook,
        );
    });
    hook.touched.iter().map(|w| w.count_ones() as usize).sum()
}
