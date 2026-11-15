//! Opening and structurally validating a persisted store.

use super::*;

/// The fixed header fields: magic, version, header size, encoding flags,
/// and the header CRC.
fn check_header(path: &Path, header: &[u8]) -> io::Result<()> {
    if &header[..8] != MAGIC {
        return Err(invalid(format!(
            "{} has unknown exact-vector magic",
            path.display()
        )));
    }
    let version = read_u32(header, 8);
    if version != VERSION {
        return Err(invalid(format!(
            "{} has exact-vector version {version}, expected {VERSION}",
            path.display()
        )));
    }
    if read_u32(header, 12) as usize != HEADER_BYTES {
        return Err(invalid(format!(
            "{} has an unsupported exact-vector header size",
            path.display()
        )));
    }
    if read_u32(header, 20) != FLAG_LITTLE_ENDIAN_F32 {
        return Err(invalid(format!(
            "{} has unsupported exact-vector encoding flags",
            path.display()
        )));
    }
    let expected_header_crc = read_u32(header, HEADER_CRC_START);
    let actual_header_crc = crate::wal::crc32(&header[..HEADER_CRC_START]);
    if actual_header_crc != expected_header_crc {
        return Err(invalid(format!(
            "{} exact-vector header CRC mismatch",
            path.display()
        )));
    }
    Ok(())
}

/// The `(dim, rows)` the header declares, checked against the file length.
fn header_shape(path: &Path, header: &[u8], file_len: usize) -> io::Result<(usize, usize)> {
    let dim = read_u32(header, 16) as usize;
    let rows = usize::try_from(read_u64(header, 24))
        .map_err(|_| invalid("exact-vector row count does not fit this platform"))?;
    let payload_bytes = usize::try_from(read_u64(header, 32))
        .map_err(|_| invalid("exact-vector payload size does not fit this platform"))?;
    let expected_payload = rows
        .checked_mul(dim)
        .and_then(|n| n.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| invalid("exact-vector dimensions overflow the file size"))?;
    if dim == 0 || payload_bytes != expected_payload {
        return Err(invalid(format!(
            "{} exact-vector shape is inconsistent: {rows}x{dim} but {payload_bytes} payload bytes",
            path.display()
        )));
    }
    let expected_len = HEADER_BYTES
        .checked_add(payload_bytes)
        .ok_or_else(|| invalid("exact-vector file size overflow"))?;
    if file_len != expected_len {
        return Err(invalid(format!(
            "{} exact-vector length is {file_len}, expected {expected_len}",
            path.display()
        )));
    }
    Ok((dim, rows))
}

impl ExactVectorStore {
    /// Open and structurally validate a persisted store without faulting its
    /// complete payload into memory. [`Self::verify_payload`] performs the
    /// explicit full SHA-256 integrity pass.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        if map.len() < HEADER_BYTES {
            return Err(invalid(format!(
                "{} is truncated: {} bytes, header requires {HEADER_BYTES}",
                path.display(),
                map.len()
            )));
        }
        let header = &map[..HEADER_BYTES];
        check_header(path, header)?;
        let (dim, rows) = header_shape(path, header, map.len())?;
        let mut payload_sha256 = [0u8; 32];
        payload_sha256.copy_from_slice(&header[HASH_START..HASH_END]);
        Ok(Self {
            storage: Storage::Mapped {
                path: path.to_path_buf(),
                map,
                dim,
                rows,
                payload_sha256,
            },
        })
    }
}
