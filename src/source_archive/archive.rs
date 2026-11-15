//! The source archive builder.

use super::*;

/// Builder for a source section. Ordinals are private to this archive.
#[derive(Debug, Default)]
pub struct SourceArchive {
    pub(super) descriptors: BTreeMap<[u8; 32], ArchiveBlob>,
    pub(super) sources: Vec<([u8; 32], ArchiveBlob)>,
    pub(super) source_ids: BTreeMap<[u8; 32], u32>,
    pub(super) rows: SharedPages<SourceRow>,
    pub(super) identities: SharedPages<SharedIdentity>,
    pub(super) identity_ids: BTreeMap<(Vec<u8>, u64), u32>,
    pub(super) identity_heads: Vec<u32>,
    pub(super) identity_links: Vec<IdentityLink>,
    pub(super) spill: Option<Mutex<File>>,
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

    pub(super) fn store_blob(&self, bytes: Vec<u8>) -> io::Result<ArchiveBlob> {
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

    pub(super) fn write_blob(&self, blob: &ArchiveBlob, writer: &mut impl Write) -> io::Result<()> {
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

    pub(super) fn blob_bytes(&self, blob: &ArchiveBlob) -> io::Result<Vec<u8>> {
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
        self.insert_checked(source, None)
    }

    pub(super) fn insert_checked(
        &mut self,
        source: &ProtobufSource,
        expected_source: Option<u32>,
    ) -> io::Result<u32> {
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
        if expected_source.is_some_and(|id| self.sources[id as usize - 1].0 != source_sha) {
            return Err(invalid(
                "logical source version has different original bytes",
            ));
        }
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
            identity: 0,
        };
        Ok(())
    }

    pub fn attach_source(
        &mut self,
        row: u32,
        source: &ProtobufSource,
        chunk_ordinal: Option<u32>,
    ) -> io::Result<()> {
        self.attach_source_with_identity(row, source, chunk_ordinal, None)
    }

    pub fn attach_source_with_identity(
        &mut self,
        row: u32,
        source: &ProtobufSource,
        chunk_ordinal: Option<u32>,
        identity: Option<&DocumentIdentity>,
    ) -> io::Result<()> {
        if self.rows.get(row as usize).is_some_and(|r| r.source != 0) {
            return Err(invalid("row already has a source"));
        }
        if let Some(identity) = identity {
            validate_identity(identity, chunk_ordinal)?;
            u32::try_from(self.identity_links.len())
                .ok()
                .and_then(|length| length.checked_add(1))
                .ok_or_else(|| invalid("identity row count exceeds u32"))?;
        }
        let existing_identity = identity.and_then(|identity| {
            self.identity_ids
                .get(&(identity.document_key.clone(), identity.version))
                .copied()
        });
        let expected_source = existing_identity.map(|id| self.identities[id as usize - 1].source);
        let id = self.insert_checked(source, expected_source)?;
        let identity = if let Some(identity) = identity {
            let key = (identity.document_key.clone(), identity.version);
            match self.identity_ids.get(&key).copied() {
                Some(ordinal) => {
                    if self.identities[ordinal as usize - 1].source != id {
                        return Err(invalid(
                            "logical source version has different original bytes",
                        ));
                    }
                    ordinal
                }
                None => {
                    let ordinal = u32::try_from(self.identities.len())
                        .ok()
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| invalid("identity count exceeds u32"))?;
                    self.identities.push(SharedIdentity {
                        document_key: identity.document_key.clone().into(),
                        version: identity.version,
                        source: id,
                    });
                    self.identity_heads.push(0);
                    self.identity_ids.insert(key, ordinal);
                    ordinal
                }
            }
        } else {
            0
        };
        self.attach(row, id, chunk_ordinal)?;
        self.rows[row as usize].identity = identity;
        if identity != 0 {
            let head = &mut self.identity_heads[identity as usize - 1];
            self.identity_links.push(IdentityLink {
                row,
                previous: *head,
            });
            *head = self.identity_links.len() as u32;
        }
        Ok(())
    }

    pub fn identity(&self, row: u32) -> Option<DocumentIdentity> {
        let row = self.rows.get(row as usize)?;
        let identity = self.identities.get(row.identity.checked_sub(1)? as usize)?;
        Some(DocumentIdentity {
            document_key: identity.document_key.to_vec(),
            version: identity.version,
            chunk_ordinal: row.chunk_ordinal,
        })
    }

    pub fn identity_snapshot(&self) -> IdentitySnapshot {
        IdentitySnapshot {
            data: IdentitySnapshotData::Building {
                rows: self.rows.clone(),
                identities: self.identities.clone(),
            },
        }
    }

    pub(super) fn index(&self, row_count: u32) -> io::Result<SourceArchiveIndex> {
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
        let mut rows = self.rows.iter().cloned().collect::<Vec<_>>();
        rows.resize_with(row_count as usize, SourceRow::default);
        let mut index = SourceArchiveIndex {
            format_version: FORMAT_VERSION,
            descriptors,
            sources,
            rows,
            identities: self
                .identities
                .iter()
                .map(|identity| SourceIdentity {
                    document_key: identity.document_key.to_vec(),
                    version: identity.version,
                    source: identity.source,
                })
                .collect(),
            identity_lookup: None,
        };
        if !index.identities.is_empty() {
            index.identity_lookup = Some(build_lookup(&index));
        }
        Ok(index)
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
        archive.identity_ids = reader
            .index
            .identities
            .iter()
            .enumerate()
            .map(|(index, identity)| {
                (
                    (identity.document_key.clone(), identity.version),
                    index as u32 + 1,
                )
            })
            .collect();
        let index = Arc::try_unwrap(reader.index).expect("new reader has no identity snapshots");
        archive.identities = index
            .identities
            .into_iter()
            .map(SharedIdentity::from)
            .collect();
        archive.rows = index.rows.into_iter().collect();
        archive.restore_identity_links();
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
