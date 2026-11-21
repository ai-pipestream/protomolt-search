//! Persisting a store and verifying its payload.

use super::*;

impl ExactVectorStore {
    /// Writing to the path this store already maps: reopen it, and refuse
    /// if the file there is now a different snapshot.
    fn reopen_same(&self, path: &Path) -> io::Result<Self> {
        let opened = Self::open(path)?;
        let same = match (&self.storage, &opened.storage) {
            (
                Storage::Mapped {
                    dim,
                    rows,
                    payload_sha256,
                    ..
                },
                Storage::Mapped {
                    dim: current_dim,
                    rows: current_rows,
                    payload_sha256: current_hash,
                    ..
                },
            ) => dim == current_dim && rows == current_rows && payload_sha256 == current_hash,
            _ => false,
        };
        if !same {
            return Err(invalid(
                "exact-vector path now refers to another snapshot; write to a new path",
            ));
        }
        Ok(opened)
    }

    /// Finalize a disk builder in place when `path` is its target: one
    /// header write, one rename. `None` when this store is not a builder
    /// for `path`.
    fn finalize_spill(&self, path: &Path, dim: usize) -> Option<io::Result<Self>> {
        let Storage::Spilled {
            target,
            path: spill,
            file,
            rows,
            digest,
            finalized,
            ..
        } = &self.storage
        else {
            return None;
        };
        if target != path {
            return None;
        }
        Some((|| {
            if finalized.load(Ordering::Acquire) {
                return Err(invalid(
                    "exact-vector builder is finalized; reopen the published image",
                ));
            }
            let payload_bytes = rows
                .checked_mul(dim)
                .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
                .ok_or_else(|| invalid("exact-vector payload size overflow"))?;
            let header = make_header(dim, *rows, payload_bytes, digest.clone().finish().0)?;
            file.write_all_at(&header, 0)?;
            file.sync_all()?;
            std::fs::rename(spill, path)?;
            // The file is now published, even if its directory sync or
            // acknowledgment fails. Never append through this file handle.
            finalized.store(true, Ordering::Release);
            #[cfg(test)]
            if FAIL_FINALIZE_SYNC.with(|fail| fail.replace(false)) {
                return Err(io::Error::other(
                    "injected exact-vector sync failure after rename",
                ));
            }
            crate::postings::fsync_parent(path)?;
            Self::open(path)
        })())
    }

    /// Stream the payload into a fresh temp file, write its header, and
    /// rename it onto `path`.
    fn write_fresh(&self, path: &Path, dim: usize) -> io::Result<()> {
        let rows = self.len();
        let payload_bytes = rows
            .checked_mul(dim)
            .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| invalid("exact-vector payload size overflow"))?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let tmp = unique_temp_path(path);
        let result = (|| -> io::Result<()> {
            let file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
            let mut out = BufWriter::new(file);
            out.write_all(&[0u8; HEADER_BYTES])?;
            let mut digest = turbo_sha256::Sha256::new();
            self.write_payload(&mut out, &mut digest)?;
            out.flush()?;
            let mut file = out
                .into_inner()
                .map_err(|e| io::Error::new(e.error().kind(), e.to_string()))?;
            let header = make_header(dim, rows, payload_bytes, digest.finish().0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header)?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)?;
            crate::postings::fsync_parent(path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Atomically persist and reopen the store. The returned instance is
    /// always mmap-backed.
    pub fn write(&self, path: &Path) -> io::Result<Self> {
        if self.path() == Some(path) {
            return self.reopen_same(path);
        }
        let dim = self
            .dim()
            .ok_or_else(|| invalid("cannot persist an exact-vector store before dim is known"))?;
        if let Some(finalized) = self.finalize_spill(path, dim) {
            return finalized;
        }
        self.write_fresh(path, dim)?;
        Self::open(path)
    }

    pub(super) fn write_payload(
        &self,
        out: &mut impl Write,
        digest: &mut turbo_sha256::Sha256,
    ) -> io::Result<()> {
        match &self.storage {
            Storage::Building { values, .. } => {
                let mut bytes = Vec::with_capacity(64 * 1024);
                for chunk in values.chunks(16 * 1024) {
                    bytes.clear();
                    for value in chunk {
                        bytes.extend_from_slice(&value.to_le_bytes());
                    }
                    digest.update(&bytes);
                    out.write_all(&bytes)?;
                }
            }
            Storage::Spilled { file, rows, .. } => {
                let payload_bytes = rows * self.dim().unwrap_or(0) * 4;
                let mut buf = vec![0u8; 1 << 20];
                let mut done = 0usize;
                while done < payload_bytes {
                    let take = buf.len().min(payload_bytes - done);
                    file.read_exact_at(&mut buf[..take], (HEADER_BYTES + done) as u64)?;
                    digest.update(&buf[..take]);
                    out.write_all(&buf[..take])?;
                    done += take;
                }
            }
            Storage::Appended { base, delta } => {
                base.write_payload(out, digest)?;
                delta.write_payload(out, digest)?;
            }
            Storage::Segmented(view) => view.write_payload(out, digest)?,
            Storage::Mapped { map, .. } => {
                let payload = &map[HEADER_BYTES..];
                digest.update(payload);
                out.write_all(payload)?;
            }
        }
        Ok(())
    }

    /// Verify the payload against the SHA-256 committed in the header.
    /// This intentionally scans the complete file and is therefore an
    /// explicit integrity operation rather than part of ordinary mmap open.
    pub fn verify_payload(&self) -> io::Result<()> {
        match &self.storage {
            Storage::Segmented(view) => return view.verify_payload(),
            Storage::Appended { base, delta } => {
                base.verify_payload()?;
                return delta.verify_payload();
            }
            Storage::Spilled { digest, .. } => {
                let mut actual = turbo_sha256::Sha256::new();
                self.write_payload(&mut io::sink(), &mut actual)?;
                return if actual.finish().0 == digest.clone().finish().0 {
                    Ok(())
                } else {
                    Err(invalid("spilled exact-vector payload SHA-256 mismatch"))
                };
            }
            _ => {}
        }
        let Storage::Mapped {
            map,
            payload_sha256,
            path,
            ..
        } = &self.storage
        else {
            return Ok(());
        };
        let actual = turbo_sha256::sha256(&map[HEADER_BYTES..]).0;
        if &actual != payload_sha256 {
            return Err(invalid(format!(
                "{} exact-vector payload SHA-256 mismatch",
                path.display()
            )));
        }
        Ok(())
    }
}
