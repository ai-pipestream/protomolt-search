//! Original protobuf bytes, interned independently of projected chunk rows.
//! The archive can be embedded in an index section; its blob area is borrowed
//! by readers so opening it does not materialize every original in memory.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use prost::Message;

use crate::pb::storage::{SourceArchiveIndex, SourceBlob, SourceRecord, SourceRow};
use crate::pb::ProtobufSource;
use crate::sha256;

const MAGIC: &[u8; 8] = b"PMSOURCE";
const HEADER_BYTES: usize = 48;
const FORMAT_VERSION: u32 = 1;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug)]
enum ArchiveBlob {
    Memory(Arc<[u8]>),
    Spill { offset: u64, length: u64 },
}

impl ArchiveBlob {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::Spill { length, .. } => *length,
        }
    }
}

/// Builder for a source section. Ordinals are private to this archive.
#[derive(Debug, Default)]
pub struct SourceArchive {
    descriptors: BTreeMap<[u8; 32], ArchiveBlob>,
    sources: Vec<([u8; 32], ArchiveBlob)>,
    source_ids: BTreeMap<[u8; 32], u32>,
    rows: Vec<SourceRow>,
    spill: Option<Mutex<File>>,
}

impl SourceArchive {
    /// Payloads live in the builder's scratch file; only addresses and row
    /// references stay in memory. Scratch bytes are copied into the image at
    /// seal and follow the caller's existing spill-directory lifecycle.
    pub fn spilling(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            spill: Some(Mutex::new(file)),
            ..Self::default()
        })
    }

    fn store_blob(&self, bytes: Vec<u8>) -> io::Result<ArchiveBlob> {
        match &self.spill {
            None => Ok(ArchiveBlob::Memory(bytes.into())),
            Some(spill) => {
                let mut file = spill
                    .lock()
                    .map_err(|_| invalid("source spill lock poisoned"))?;
                let offset = file.seek(SeekFrom::End(0))?;
                file.write_all(&bytes)?;
                Ok(ArchiveBlob::Spill {
                    offset,
                    length: bytes.len() as u64,
                })
            }
        }
    }

    fn write_blob(&self, blob: &ArchiveBlob, writer: &mut impl Write) -> io::Result<()> {
        match blob {
            ArchiveBlob::Memory(bytes) => writer.write_all(bytes),
            ArchiveBlob::Spill { offset, length } => {
                let mut file = self
                    .spill
                    .as_ref()
                    .ok_or_else(|| invalid("source spill missing"))?
                    .lock()
                    .map_err(|_| invalid("source spill lock poisoned"))?;
                file.seek(SeekFrom::Start(*offset))?;
                let copied = io::copy(&mut (&mut *file).take(*length), writer)?;
                if copied != *length {
                    return Err(invalid("source spill truncated"));
                }
                Ok(())
            }
        }
    }

    fn blob_bytes(&self, blob: &ArchiveBlob) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_blob(blob, &mut bytes)?;
        Ok(bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Retain one original independently of whether it produces any rows.
    /// The caller validates schema and indexing policy; this layer preserves
    /// opaque bytes, including shapes the current decoder cannot interpret.
    pub fn insert(&mut self, source: &ProtobufSource) -> io::Result<u32> {
        if source.descriptor_set.is_empty() || source.message_type.is_empty() {
            return Err(invalid(
                "source requires descriptor bytes and a message type",
            ));
        }
        let descriptor_sha = sha256::digest(&source.descriptor_set);
        let record = SourceRecord {
            descriptor_sha256: descriptor_sha.to_vec(),
            message_type: source.message_type.clone(),
            payload: source.payload.clone(),
        }
        .encode_to_vec();
        let source_sha = sha256::digest(&record);
        if let Some(bytes) = self.descriptors.get(&descriptor_sha) {
            if self.blob_bytes(bytes)? != source.descriptor_set {
                return Err(invalid("descriptor content-address collision"));
            }
        }
        if let Some(&id) = self.source_ids.get(&source_sha) {
            if self.blob_bytes(&self.sources[id as usize - 1].1)? != record {
                return Err(invalid("source content-address collision"));
            }
            return Ok(id);
        }
        let id = u32::try_from(self.sources.len())
            .ok()
            .and_then(|v| v.checked_add(1))
            .ok_or_else(|| invalid("source count exceeds u32"))?;
        if !self.descriptors.contains_key(&descriptor_sha) {
            let blob = self.store_blob(source.descriptor_set.clone())?;
            self.descriptors.insert(descriptor_sha, blob);
        }
        let blob = self.store_blob(record)?;
        self.sources.push((source_sha, blob));
        self.source_ids.insert(source_sha, id);
        Ok(id)
    }

    pub fn attach(&mut self, row: u32, source: u32, chunk_ordinal: Option<u32>) -> io::Result<()> {
        if source == 0 || source as usize > self.sources.len() {
            return Err(invalid("source ordinal is out of range"));
        }
        let row = row as usize;
        if self.rows.get(row).is_some_and(|r| r.source != 0) {
            return Err(invalid("row already has a source"));
        }
        self.rows
            .resize_with(self.rows.len().max(row + 1), SourceRow::default);
        self.rows[row] = SourceRow {
            source,
            chunk_ordinal,
        };
        Ok(())
    }

    pub fn attach_source(
        &mut self,
        row: u32,
        source: &ProtobufSource,
        chunk_ordinal: Option<u32>,
    ) -> io::Result<()> {
        if self.rows.get(row as usize).is_some_and(|r| r.source != 0) {
            return Err(invalid("row already has a source"));
        }
        let id = self.insert(source)?;
        self.attach(row, id, chunk_ordinal)
    }

    fn index(&self, row_count: u32) -> io::Result<SourceArchiveIndex> {
        if self.rows.len() > row_count as usize {
            return Err(invalid("source rows exceed index rows"));
        }
        let mut offset = 0u64;
        let mut address = |(digest, bytes): (&[u8; 32], &ArchiveBlob)| {
            let result = SourceBlob {
                sha256: digest.to_vec(),
                offset,
                length: bytes.len(),
            };
            offset = offset
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("source archive size overflows"))?;
            Ok(result)
        };
        let descriptors = self
            .descriptors
            .iter()
            .map(&mut address)
            .collect::<io::Result<Vec<_>>>()?;
        let sources = self
            .sources
            .iter()
            .map(|(hash, bytes)| address((hash, bytes)))
            .collect::<io::Result<Vec<_>>>()?;
        let mut rows = self.rows.clone();
        rows.resize_with(row_count as usize, SourceRow::default);
        Ok(SourceArchiveIndex {
            format_version: FORMAT_VERSION,
            descriptors,
            sources,
            rows,
        })
    }

    pub fn encoded_len(&self, row_count: u32) -> io::Result<u64> {
        let index = self.index(row_count)?;
        let blobs = index
            .descriptors
            .iter()
            .chain(&index.sources)
            .try_fold(0u64, |sum, b| {
                sum.checked_add(b.length)
                    .ok_or_else(|| invalid("source archive size overflows"))
            })?;
        (HEADER_BYTES as u64)
            .checked_add(index.encoded_len() as u64)
            .and_then(|v| v.checked_add(blobs))
            .ok_or_else(|| invalid("source archive size overflows"))
    }

    pub fn write(&self, writer: &mut impl Write, row_count: u32) -> io::Result<()> {
        let index = self.index(row_count)?.encode_to_vec();
        writer.write_all(MAGIC)?;
        writer.write_all(&(index.len() as u64).to_le_bytes())?;
        writer.write_all(&sha256::digest(&index))?;
        writer.write_all(&index)?;
        for bytes in self
            .descriptors
            .values()
            .chain(self.sources.iter().map(|(_, b)| b))
        {
            self.write_blob(bytes, writer)?;
        }
        Ok(())
    }

    pub fn read(bytes: &[u8], row_count: u32) -> io::Result<Self> {
        let reader = SourceArchiveReader::open(bytes, row_count)?;
        let mut archive = Self::default();
        for source in 1..=reader.index.sources.len() as u32 {
            let id = archive.insert(&reader.source(bytes, source)?)?;
            if id != source {
                return Err(invalid("duplicate source record"));
            }
        }
        archive.rows = reader.index.rows;
        Ok(archive)
    }

    pub fn row(&self, row: u32) -> io::Result<Option<(ProtobufSource, Option<u32>)>> {
        let Some(reference) = self.rows.get(row as usize).filter(|r| r.source != 0) else {
            return Ok(None);
        };
        let record = SourceRecord::decode(
            self.blob_bytes(&self.sources[reference.source as usize - 1].1)?
                .as_slice(),
        )
        .map_err(|e| invalid(format!("source record: {e}")))?;
        let digest: [u8; 32] = record
            .descriptor_sha256
            .as_slice()
            .try_into()
            .map_err(|_| invalid("invalid descriptor content address"))?;
        let descriptor = self
            .descriptors
            .get(&digest)
            .ok_or_else(|| invalid("source descriptor is missing"))?;
        Ok(Some((
            ProtobufSource {
                descriptor_set: self.blob_bytes(descriptor)?,
                message_type: record.message_type,
                payload: record.payload,
            },
            reference.chunk_ordinal,
        )))
    }
}

/// Parsed index over borrowed source bytes, suitable for a mapped section.
#[derive(Debug)]
pub struct SourceArchiveReader {
    index: SourceArchiveIndex,
    blobs_offset: usize,
    archive_len: usize,
    descriptors: BTreeMap<Vec<u8>, usize>,
}

impl SourceArchiveReader {
    pub fn open(bytes: &[u8], row_count: u32) -> io::Result<Self> {
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
        if index.rows.len() != row_count as usize {
            return Err(invalid("source archive row count differs from the index"));
        }
        let blobs = &bytes[index_end..];
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
        for row in &index.rows {
            if row.source as usize > index.sources.len()
                || (row.source == 0 && row.chunk_ordinal.is_some())
            {
                return Err(invalid("invalid source row reference"));
            }
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
            index,
            blobs_offset: index_end,
            archive_len: bytes.len(),
            descriptors,
        })
    }

    fn blob<'a>(&self, archive: &'a [u8], blob: &SourceBlob) -> io::Result<&'a [u8]> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> ProtobufSource {
        ProtobufSource {
            descriptor_set: include_bytes!("../tests/fixtures/protobuf-semantics/descriptor.bin")
                .to_vec(),
            message_type: "semantics.Doc".into(),
            // Noncanonical varint and an unknown field, retained without decode.
            payload: vec![8, 0x81, 0, 0xa0, 6, 99],
        }
    }

    #[test]
    fn shared_sources_preserve_exact_bytes_and_sparse_chunk_references() {
        let source = source();
        let mut archive = SourceArchive::default();
        let id = archive.insert(&source).unwrap();
        assert_eq!(archive.insert(&source).unwrap(), id);
        archive.attach(0, id, Some(0)).unwrap();
        archive.attach(2, id, Some(1)).unwrap();
        assert!(archive.attach(0, id, None).is_err());
        assert!(archive.attach(3, 99, None).is_err());
        let mut bytes = Vec::new();
        archive.write(&mut bytes, 4).unwrap();
        assert_eq!(bytes.len() as u64, archive.encoded_len(4).unwrap());
        let reader = SourceArchiveReader::open(&bytes, 4).unwrap();
        assert_eq!(reader.index.sources.len(), 1);
        assert_eq!(reader.index.descriptors.len(), 1);
        for (row, ordinal) in [(0, 0), (2, 1)] {
            assert_eq!(
                reader.row(&bytes, row).unwrap(),
                Some((source.clone(), Some(ordinal)))
            );
        }
        assert!(reader.row(&bytes, 1).unwrap().is_none());
        assert!(reader.row(&bytes, 3).unwrap().is_none());
        assert!(reader.row(&bytes, 4).is_err());
        reader.verify(&bytes).unwrap();
        let restored = SourceArchive::read(&bytes, 4).unwrap();
        let mut rewritten = Vec::new();
        restored.write(&mut rewritten, 4).unwrap();
        assert_eq!(rewritten, bytes);
    }

    #[test]
    fn originals_without_rows_and_reused_descriptors_survive() {
        let mut source = source();
        let mut archive = SourceArchive::default();
        let first = archive.insert(&source).unwrap();
        source.payload.clear();
        let second = archive.insert(&source).unwrap();
        let mut bytes = Vec::new();
        archive.write(&mut bytes, 0).unwrap();
        let reader = SourceArchiveReader::open(&bytes, 0).unwrap();
        assert_eq!(reader.index.sources.len(), 2);
        assert_eq!(reader.index.descriptors.len(), 1);
        assert_ne!(
            reader.source(&bytes, first).unwrap().payload,
            source.payload
        );
        assert_eq!(reader.source(&bytes, second).unwrap(), source);
    }

    #[test]
    fn every_truncation_and_byte_corruption_is_detected() {
        let mut archive = SourceArchive::default();
        let id = archive.insert(&source()).unwrap();
        archive.attach(0, id, None).unwrap();
        let mut bytes = Vec::new();
        archive.write(&mut bytes, 1).unwrap();
        for at in 0..bytes.len() {
            let truncated = &bytes[..at];
            assert!(
                SourceArchiveReader::open(truncated, 1)
                    .and_then(|r| r.verify(truncated))
                    .is_err(),
                "truncate {at}"
            );
            let mut corrupted = bytes.clone();
            corrupted[at] ^= 1;
            assert!(
                SourceArchiveReader::open(&corrupted, 1)
                    .and_then(|r| r.verify(&corrupted))
                    .is_err(),
                "flip {at}"
            );
        }
        assert!(SourceArchiveReader::open(&bytes, 2).is_err());
        bytes.push(0);
        assert!(SourceArchiveReader::open(&bytes, 1).is_err());
    }
}
