//! Appending rows to a store.

use super::*;

impl ExactVectorStore {
    /// Refuse a batch that is misshapen, non-finite, of another dimension,
    /// or that would overflow the store's byte size.
    fn check_append(&self, vectors: &[f32], dim: usize) -> io::Result<()> {
        validate_shape(dim, vectors.len())?;
        if let Some((index, value)) = vectors
            .iter()
            .copied()
            .enumerate()
            .find(|(_, v)| !v.is_finite())
        {
            return Err(invalid(format!(
                "exact vector coordinate {index} is not finite: {value}"
            )));
        }
        if self.dim().is_some_and(|known| known != dim) {
            return Err(invalid(format!(
                "exact-vector append dim {dim} does not match store dim {}",
                self.dim().expect("checked Some")
            )));
        }
        self.len()
            .checked_add(vectors.len() / dim)
            .and_then(|rows| rows.checked_mul(dim))
            .and_then(|values| values.checked_mul(4))
            .ok_or_else(|| invalid("exact-vector append size overflow"))?;
        Ok(())
    }

    /// Convert an immutable store into base plus a private spill holding
    /// `vectors`. The readable view changes only if the whole append succeeds.
    fn append_over_immutable(
        &mut self,
        origin: &Path,
        vectors: &[f32],
        dim: usize,
    ) -> io::Result<()> {
        let target = unique_temp_path(origin);
        let mut delta = Self::spilling(&target, Some(dim))?;
        delta.append(vectors, dim)?;
        let base = Self {
            storage: std::mem::replace(
                &mut self.storage,
                Storage::Building {
                    dim: None,
                    values: Vec::new(),
                },
            ),
        };
        self.storage = Storage::Appended {
            base: Box::new(base),
            delta: Box::new(delta),
        };
        Ok(())
    }

    /// Append complete rows. A mapped store retains its immutable payload and
    /// writes only the new rows to a private spill. A finalized builder refuses
    /// writes; callers continue through the mapped instance returned by write.
    pub fn append(&mut self, vectors: &[f32], dim: usize) -> io::Result<()> {
        if vectors.is_empty() {
            return Ok(());
        }
        self.check_append(vectors, dim)?;
        let append_origin = match &self.storage {
            Storage::Mapped { path, .. } => Some(path.clone()),
            Storage::Segmented(view) => Some(view.append_target.clone()),
            _ => None,
        };
        if let Some(path) = append_origin {
            return self.append_over_immutable(&path, vectors, dim);
        }
        match &mut self.storage {
            Storage::Building { dim: known, values } => {
                *known = Some(dim);
                values.extend_from_slice(vectors);
            }
            Storage::Spilled {
                file,
                dim: known,
                rows,
                digest,
                finalized,
                ..
            } => {
                if finalized.load(Ordering::Acquire) {
                    return Err(invalid("exact-vector builder is finalized; append through the returned mapped store"));
                }
                let mut bytes = Vec::with_capacity(vectors.len() * 4);
                for value in vectors {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                let offset = HEADER_BYTES as u64 + (*rows as u64) * (dim as u64) * 4;
                file.write_all_at(&bytes, offset)?;
                // A previous failed larger write may have left an uncommitted
                // suffix. The successful append defines the complete extent.
                file.set_len(offset + bytes.len() as u64)?;
                digest.update(&bytes);
                *known = Some(dim);
                *rows += vectors.len() / dim;
            }
            Storage::Appended { delta, .. } => delta.append(vectors, dim)?,
            Storage::Mapped { .. } | Storage::Segmented(_) => {
                unreachable!("immutable store converted above")
            }
        }
        Ok(())
    }
}
