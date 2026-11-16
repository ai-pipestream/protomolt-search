//! What a walk reports as it goes.

/// What a walk reports as it goes: every exactly scored allowed node at
/// layer 0 and each expansion. A plain search uses [`NoHook`].
pub trait Hook {
    /// An allowed node was scored at layer 0: its id, the traversal
    /// kernel's score, and its row (for the scalar rescoring of anything
    /// that leaves the provider).
    fn scored(&mut self, node: u32, score: f32, row: &[f32]);
    /// After each expansion; `false` stops the walk.
    fn hop(&mut self) -> bool;
    /// Current exact score floor for layer-0 floor pruning.
    #[inline]
    fn current_floor(&self) -> f32 {
        f32::NEG_INFINITY
    }
}

pub struct NoHook;

impl Hook for NoHook {
    #[inline]
    fn scored(&mut self, _node: u32, _score: f32, _row: &[f32]) {}
    #[inline]
    fn hop(&mut self) -> bool {
        true
    }
}

/// Unary walk hook carrying a static score floor.
pub struct FloorHook(pub f32);

impl Hook for FloorHook {
    #[inline]
    fn scored(&mut self, _node: u32, _score: f32, _row: &[f32]) {}
    #[inline]
    fn hop(&mut self) -> bool {
        true
    }
    #[inline]
    fn current_floor(&self) -> f32 {
        self.0
    }
}
