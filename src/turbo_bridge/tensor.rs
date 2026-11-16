//! Checked row-major tensor views with explicit strides.

use std::fmt;

/// A borrowed row-major matrix. Each row starts at `i * stride` and contains
/// `dim` elements; a prefix of a packed matrix therefore remains zero-copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixView<'a, T: Copy = f32> {
    data: &'a [T],
    rows: usize,
    dim: usize,
    stride: usize,
}

impl<'a, T: Copy> MatrixView<'a, T> {
    /// Interpret a contiguous `[rows, dim]` buffer without copying it.
    pub fn packed(data: &'a [T], dim: usize) -> Result<Self, TensorError> {
        if dim == 0 {
            return Err(TensorError::ZeroDim);
        }
        if !data.len().is_multiple_of(dim) {
            return Err(TensorError::RaggedFlat {
                len: data.len(),
                dim,
            });
        }
        Ok(Self {
            data,
            rows: data.len() / dim,
            dim,
            stride: dim,
        })
    }

    /// View `rows` rows whose starts are `stride` elements apart.
    pub fn strided(
        data: &'a [T],
        rows: usize,
        dim: usize,
        stride: usize,
    ) -> Result<Self, TensorError> {
        if dim == 0 {
            return Err(TensorError::ZeroDim);
        }
        if stride < dim {
            return Err(TensorError::StrideTooSmall { dim, stride });
        }
        let required = if rows == 0 {
            0
        } else {
            (rows - 1)
                .checked_mul(stride)
                .and_then(|offset| offset.checked_add(dim))
                .unwrap_or(usize::MAX)
        };
        if data.len() < required {
            return Err(TensorError::RaggedFlat {
                len: data.len(),
                dim: required,
            });
        }
        Ok(Self {
            data,
            rows,
            dim,
            stride,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn row(&self, index: usize) -> Option<&'a [T]> {
        if index >= self.rows {
            return None;
        }
        let start = index * self.stride;
        Some(&self.data[start..start + self.dim])
    }

    pub fn iter_rows(&self) -> impl Iterator<Item = &'a [T]> + 'a {
        let (data, rows, dim, stride) = (self.data, self.rows, self.dim, self.stride);
        (0..rows).map(move |index| {
            let start = index * stride;
            &data[start..start + dim]
        })
    }

    /// Borrow the leftmost `dim` columns of every row.
    pub fn prefix(&self, dim: usize) -> Result<Self, TensorError> {
        if dim == 0 {
            return Err(TensorError::ZeroDim);
        }
        if dim > self.dim {
            return Err(TensorError::StrideTooSmall {
                dim,
                stride: self.dim,
            });
        }
        Ok(Self {
            data: self.data,
            rows: self.rows,
            dim,
            stride: self.stride,
        })
    }

    /// Pack the visible rows into owned storage, copying only here.
    pub fn to_owned(&self) -> OwnedMatrix<T> {
        let mut data = Vec::with_capacity(self.rows * self.dim);
        for row in self.iter_rows() {
            data.extend_from_slice(row);
        }
        OwnedMatrix {
            data,
            dim: self.dim,
            empty_rows: Vec::new(),
        }
    }
}

/// An owned packed matrix and the rows that contain no usable direction.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedMatrix<T: Copy = f32> {
    data: Vec<T>,
    dim: usize,
    empty_rows: Vec<u32>,
}

impl<T: Copy> OwnedMatrix<T> {
    pub fn from_flat(data: Vec<T>, dim: usize) -> Result<Self, TensorError> {
        let view = MatrixView::packed(&data, dim)?;
        Ok(view.to_owned())
    }

    pub fn rows(&self) -> usize {
        self.data.len() / self.dim
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn data(&self) -> &[T] {
        &self.data
    }

    pub fn empty_rows(&self) -> &[u32] {
        &self.empty_rows
    }

    pub fn view(&self) -> MatrixView<'_, T> {
        MatrixView::packed(&self.data, self.dim).expect("an owned matrix is always packed")
    }
}

impl OwnedMatrix<f32> {
    /// Normalize each non-zero row in place, retaining and recording zero rows.
    pub fn l2_normalize_rows(&mut self) {
        for (index, row) in self.data.chunks_exact_mut(self.dim).enumerate() {
            let norm = row
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if norm == 0.0 {
                let index = index as u32;
                if !self.empty_rows.contains(&index) {
                    self.empty_rows.push(index);
                }
            } else {
                for value in row {
                    *value = (*value as f64 / norm) as f32;
                }
            }
        }
    }
}

/// Tensor shape validation failures, all reported as invalid caller input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TensorError {
    ZeroDim,
    RaggedFlat { len: usize, dim: usize },
    StrideTooSmall { dim: usize, stride: usize },
}

impl fmt::Display for TensorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDim => f.write_str("tensor dimension must be greater than zero"),
            Self::RaggedFlat { len, dim } => {
                write!(
                    f,
                    "tensor data length {len} is incompatible with span {dim}"
                )
            }
            Self::StrideTooSmall { dim, stride } => {
                write!(f, "tensor stride {stride} is smaller than dimension {dim}")
            }
        }
    }
}

impl std::error::Error for TensorError {}

impl From<TensorError> for tonic::Status {
    fn from(error: TensorError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}
