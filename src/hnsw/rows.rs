//! The FP32 rows the graph traverses and scores, behind a trait that carries
//! the identity a shared row store must match.
//!
//! In this milestone the provider owns its rows: [`OwnedRows`] on the heap,
//! [`MappedRows`] over the image's row section when the index is served from
//! a memory map. #71 binds the trait to the product's `.exact` sidecar, which
//! keeps the same rows; the identity here is what that binding checks: the
//! dimension, the row count, and the SHA-256 of the rows' little-endian
//! bytes in row order, the digest `ExactVectorStore` keeps over its payload.

use crate::sha256::Sha256;

/// What a later row store must match before it can replace the rows the
/// graph was built over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSourceIdentity {
    pub dim: usize,
    pub rows: usize,
    /// SHA-256 over every row's little-endian `f32` bytes, row by row.
    pub content_digest: [u8; 32],
}

/// Row-major FP32 rows of one dimension.
pub trait RowSource: Sync {
    fn dim(&self) -> usize;

    fn row_count(&self) -> usize;

    /// Every row, row-major, `dim` values each. The walk reads from this
    /// slice directly; the trait does not sit in the inner loop.
    fn values(&self) -> &[f32];

    #[inline]
    fn row(&self, row: u32) -> &[f32] {
        let dim = self.dim();
        let at = row as usize * dim;
        &self.values()[at..at + dim]
    }

    /// The digest of [`RowSourceIdentity`], computed over the rows.
    fn content_digest(&self) -> [u8; 32] {
        let mut sha = Sha256::new();
        let mut bytes = Vec::with_capacity(4096);
        for chunk in self.values().chunks(1024) {
            bytes.clear();
            for value in chunk {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            sha.update(&bytes);
        }
        sha.finalize()
    }

    fn identity(&self) -> RowSourceIdentity {
        RowSourceIdentity {
            dim: self.dim(),
            rows: self.row_count(),
            content_digest: self.content_digest(),
        }
    }
}

/// The provider's own copy of its rows.
#[derive(Debug, Clone)]
pub struct OwnedRows {
    dim: usize,
    values: Vec<f32>,
}

impl OwnedRows {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            values: Vec::new(),
        }
    }

    /// Rows already laid out row-major with `dim` values each.
    pub fn from_values(dim: usize, values: Vec<f32>) -> Self {
        assert!(dim > 0 && values.len().is_multiple_of(dim));
        Self { dim, values }
    }

    pub fn reserve(&mut self, values: usize) {
        self.values.reserve(values);
    }

    /// Append one row of `dim` values.
    pub fn push(&mut self, row: &[f32]) {
        debug_assert_eq!(row.len(), self.dim);
        self.values.extend_from_slice(row);
    }
}

impl RowSource for OwnedRows {
    #[inline]
    fn dim(&self) -> usize {
        self.dim
    }

    #[inline]
    fn row_count(&self) -> usize {
        self.values.len() / self.dim
    }

    #[inline]
    fn values(&self) -> &[f32] {
        &self.values
    }
}

/// Rows read in place from a mapped image.
#[derive(Debug, Clone, Copy)]
pub struct MappedRows<'a> {
    dim: usize,
    values: &'a [f32],
}

impl<'a> MappedRows<'a> {
    pub fn new(dim: usize, values: &'a [f32]) -> Self {
        assert!(dim > 0 && values.len().is_multiple_of(dim));
        Self { dim, values }
    }

    /// The mapped values with the map's lifetime rather than this view's.
    pub fn into_values(self) -> &'a [f32] {
        self.values
    }
}

impl RowSource for MappedRows<'_> {
    #[inline]
    fn dim(&self) -> usize {
        self.dim
    }

    #[inline]
    fn row_count(&self) -> usize {
        self.values.len() / self.dim
    }

    #[inline]
    fn values(&self) -> &[f32] {
        self.values
    }
}
