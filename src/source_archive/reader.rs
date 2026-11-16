//! The reader over a persisted, borrowed source archive.

use super::*;

/// The checksummed index and the offset where the blob area begins.
fn decode_index(bytes: &[u8], row_count: u32) -> io::Result<(SourceArchiveIndex, usize)> {
    if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
        return Err(invalid("invalid source archive header"));
    }
    let index_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let index_end = usize::try_from(index_len)
        .ok()
        .and_then(|v| HEADER_BYTES.checked_add(v))
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| invalid("source index is truncated"))?;
    let encoded_index = &bytes[HEADER_BYTES..index_end];
    if sha256::digest(encoded_index).as_slice() != &bytes[16..48] {
        return Err(invalid("source index checksum mismatch"));
    }
    let index = SourceArchiveIndex::decode(encoded_index)
        .map_err(|e| invalid(format!("source index: {e}")))?;
    if index.format_version != FORMAT_VERSION {
        return Err(invalid("unsupported source archive version"));
    }
    if index.identities.is_empty() != index.identity_lookup.is_none() {
        return Err(invalid(
            "source archive identity lookup must be present exactly when identities are",
        ));
    }
    if index.rows.len() != row_count as usize {
        return Err(invalid("source archive row count differs from the index"));
    }
    Ok((index, index_end))
}

/// Descriptor and source blobs tile the blob area exactly, in order.
fn check_blob_addresses(index: &SourceArchiveIndex, blobs: &[u8]) -> io::Result<()> {
    let mut offset = 0u64;
    for blob in index.descriptors.iter().chain(&index.sources) {
        if blob.sha256.len() != 32 || blob.offset != offset || blob.length == 0 {
            return Err(invalid("invalid source blob address"));
        }
        offset = offset
            .checked_add(blob.length)
            .filter(|v| *v <= blobs.len() as u64)
            .ok_or_else(|| invalid("source blob is truncated"))?;
    }
    if offset != blobs.len() as u64 {
        return Err(invalid("source archive has trailing bytes"));
    }
    Ok(())
}

/// Rows refer to existing sources and identities, and identities are
/// well formed and unique by key and version.
fn check_references(index: &SourceArchiveIndex) -> io::Result<()> {
    for row in &index.rows {
        if row.source as usize > index.sources.len()
            || (row.source == 0 && row.chunk_ordinal.is_some())
            || row.identity as usize > index.identities.len()
        {
            return Err(invalid("invalid source row reference"));
        }
        if row.identity != 0 && index.identities[row.identity as usize - 1].source != row.source {
            return Err(invalid("row identity refers to a different source"));
        }
    }
    let mut identities = std::collections::BTreeSet::new();
    for identity in &index.identities {
        if identity.document_key.is_empty()
            || identity.document_key.len() > 16 * 1024
            || identity.version == 0
            || identity.source == 0
            || identity.source as usize > index.sources.len()
            || !identities.insert((&identity.document_key, identity.version))
        {
            return Err(invalid("invalid or duplicate logical source identity"));
        }
    }
    Ok(())
}

/// Parsed index over borrowed source bytes, suitable for a mapped section.
#[derive(Debug)]
pub struct SourceArchiveReader {
    pub(super) index: Arc<SourceArchiveIndex>,
    pub(super) blobs_offset: usize,
    pub(super) archive_len: usize,
    pub(super) descriptors: BTreeMap<Vec<u8>, usize>,
}

impl SourceArchiveReader {
    pub fn open(bytes: &[u8], row_count: u32) -> io::Result<Self> {
        let (index, index_end) = decode_index(bytes, row_count)?;
        check_blob_addresses(&index, &bytes[index_end..])?;
        check_references(&index)?;
        if index.identity_lookup.is_some() {
            validate_lookup(&index)?;
        }
        let descriptors: BTreeMap<_, _> = index
            .descriptors
            .iter()
            .enumerate()
            .map(|(i, b)| (b.sha256.clone(), i))
            .collect();
        if descriptors.len() != index.descriptors.len() {
            return Err(invalid("duplicate source descriptor"));
        }
        Ok(Self {
            index: Arc::new(index),
            blobs_offset: index_end,
            archive_len: bytes.len(),
            descriptors,
        })
    }

    pub(super) fn blob<'a>(&self, archive: &'a [u8], blob: &SourceBlob) -> io::Result<&'a [u8]> {
        if archive.len() != self.archive_len {
            return Err(invalid("source archive length changed"));
        }
        let bytes = &archive[self.blobs_offset + blob.offset as usize
            ..self.blobs_offset + (blob.offset + blob.length) as usize];
        if sha256::digest(bytes).as_slice() != blob.sha256 {
            return Err(invalid("source blob checksum mismatch"));
        }
        Ok(bytes)
    }

    pub fn source(&self, bytes: &[u8], ordinal: u32) -> io::Result<ProtobufSource> {
        let blob = ordinal
            .checked_sub(1)
            .and_then(|id| self.index.sources.get(id as usize))
            .ok_or_else(|| invalid("source ordinal is out of range"))?;
        let record = SourceRecord::decode(self.blob(bytes, blob)?)
            .map_err(|e| invalid(format!("source record: {e}")))?;
        if record.message_type.is_empty() {
            return Err(invalid("source message type is empty"));
        }
        let descriptor = self
            .descriptors
            .get(&record.descriptor_sha256)
            .ok_or_else(|| invalid("source descriptor is missing"))?;
        Ok(ProtobufSource {
            descriptor_set: self
                .blob(bytes, &self.index.descriptors[*descriptor])?
                .to_vec(),
            message_type: record.message_type,
            payload: record.payload,
        })
    }

    pub fn row(&self, bytes: &[u8], row: u32) -> io::Result<Option<(ProtobufSource, Option<u32>)>> {
        let reference = self
            .index
            .rows
            .get(row as usize)
            .ok_or_else(|| invalid("source row is out of range"))?;
        if reference.source == 0 {
            return Ok(None);
        }
        Ok(Some((
            self.source(bytes, reference.source)?,
            reference.chunk_ordinal,
        )))
    }

    pub fn identity(&self, row: u32) -> Option<DocumentIdentity> {
        row_identity(&self.index.rows, &self.index.identities, row)
    }

    pub fn identity_snapshot(&self) -> IdentitySnapshot {
        IdentitySnapshot {
            data: IdentitySnapshotData::Encoded(Arc::clone(&self.index)),
        }
    }

    #[cfg(test)]
    pub fn verify(&self, bytes: &[u8]) -> io::Result<()> {
        for descriptor in &self.index.descriptors {
            self.blob(bytes, descriptor)?;
        }
        for source in 1..=self.index.sources.len() as u32 {
            self.source(bytes, source)?;
        }
        Ok(())
    }
}
