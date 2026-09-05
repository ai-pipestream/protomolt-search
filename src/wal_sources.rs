//! Generation-local source interning. Row records carry addresses, while WAL
//! readers restore self-contained documents for replay and replication.

use super::{read_frame, read_frame_limited, write_frame, FrameRead};
use crate::pb::storage::{SourceBlob, SourceRecord};
use crate::pb::wal::{source_wal_blob, LoggedAddDocuments, SourceReference, SourceWalBlob};
use crate::pb::ProtobufSource;
use crate::sha256;
use prost::Message;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"PMSWAL01";
const NAME: &str = "sources.wal";

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn header(file: &mut File) -> io::Result<()> {
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("invalid WAL source header"));
    }
    Ok(())
}

fn validate(blob: &SourceWalBlob) -> io::Result<()> {
    match &blob.value {
        Some(source_wal_blob::Value::Descriptor(bytes)) if !bytes.is_empty() => Ok(()),
        Some(source_wal_blob::Value::Source(record))
            if record.descriptor_sha256.len() == 32 && !record.message_type.is_empty() =>
        {
            Ok(())
        }
        _ => Err(invalid("invalid WAL source blob")),
    }
}

pub(super) struct Writer {
    file: File,
    addresses: HashMap<[u8; 32], SourceBlob>,
    end: u64,
}

impl Writer {
    pub(super) fn open(dir: &Path) -> io::Result<Self> {
        let path = dir.join(NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        // An interrupted header cannot have a referencing row: the writer
        // finishes each blob before it appends any such row.
        if file.metadata()?.len() < MAGIC.len() as u64 {
            file.set_len(0)?;
            file.write_all(MAGIC)?;
        } else {
            header(&mut file)?;
        }
        let mut addresses = HashMap::new();
        let mut end = MAGIC.len() as u64;
        let file_len = file.metadata()?.len();
        loop {
            let start = end;
            // A length prefix in an incomplete tail must not allocate more
            // than this file holds. Referencing row readers still refuse it.
            let remaining = file_len - start;
            if remaining == 0 {
                break;
            }
            let mut length = [0; 4];
            let torn = if remaining < 8 {
                true
            } else {
                file.read_exact(&mut length)?;
                file.seek(SeekFrom::Start(start))?;
                u64::from(u32::from_le_bytes(length)) > remaining - 8
            };
            if torn {
                file.set_len(start)?;
                file.seek(SeekFrom::Start(start))?;
                break;
            }
            match read_frame(&mut file, &mut end)? {
                FrameRead::Frame(bytes) => {
                    let blob = SourceWalBlob::decode(bytes.as_slice()).map_err(io::Error::other)?;
                    validate(&blob)?;
                    let hash = sha256::digest(&bytes);
                    if addresses
                        .insert(
                            hash,
                            SourceBlob {
                                sha256: hash.to_vec(),
                                offset: start,
                                length: end - start,
                            },
                        )
                        .is_some()
                    {
                        return Err(invalid("duplicate WAL source blob"));
                    }
                }
                FrameRead::End => break,
                FrameRead::Torn { .. } => {
                    end = start;
                    file.set_len(end)?;
                    file.seek(SeekFrom::Start(end))?;
                    break;
                }
            }
        }
        Ok(Self {
            file,
            addresses,
            end,
        })
    }

    fn intern(&mut self, value: source_wal_blob::Value) -> io::Result<SourceBlob> {
        let blob = SourceWalBlob { value: Some(value) };
        validate(&blob)?;
        let bytes = blob.encode_to_vec();
        let hash = sha256::digest(&bytes);
        if let Some(address) = self.addresses.get(&hash) {
            return Ok(address.clone());
        }
        if bytes.len() > u32::MAX as usize {
            return Err(invalid("WAL source blob exceeds frame size"));
        }
        let address = SourceBlob {
            sha256: hash.to_vec(),
            offset: self.end,
            length: bytes.len() as u64 + 8,
        };
        if let Err(error) = write_frame(&mut self.file, &bytes) {
            self.file.set_len(self.end)?;
            self.file.seek(SeekFrom::Start(self.end))?;
            return Err(error);
        }
        self.end += address.length;
        self.addresses.insert(hash, address.clone());
        Ok(address)
    }

    pub(super) fn intern_documents(&mut self, batch: &mut LoggedAddDocuments) -> io::Result<()> {
        if !batch.source_references.is_empty() {
            return Err(invalid("WAL append requires resolved source references"));
        }
        let mut references = Vec::with_capacity(batch.documents.len());
        for document in &mut batch.documents {
            let Some(source) = document.original_source.take() else {
                if document.source_chunk_ordinal.is_some() {
                    return Err(invalid("chunk ordinal has no original source"));
                }
                references.push(SourceReference::default());
                continue;
            };
            let descriptor_hash = sha256::digest(&source.descriptor_set);
            let descriptor =
                self.intern(source_wal_blob::Value::Descriptor(source.descriptor_set))?;
            let source = self.intern(source_wal_blob::Value::Source(SourceRecord {
                descriptor_sha256: descriptor_hash.to_vec(),
                message_type: source.message_type,
                payload: source.payload,
            }))?;
            references.push(SourceReference {
                descriptor: Some(descriptor),
                source: Some(source),
            });
        }
        batch.source_references = references;
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

pub(super) struct Reader {
    file: File,
}

impl Reader {
    pub(super) fn open(dir: &Path) -> io::Result<Self> {
        let mut file = File::open(dir.join(NAME))?;
        header(&mut file)?;
        Ok(Self { file })
    }

    fn blob(&mut self, address: &SourceBlob) -> io::Result<SourceWalBlob> {
        let file_len = self.file.metadata()?.len();
        if address.sha256.len() != 32
            || address.offset < MAGIC.len() as u64
            || address.length < 8
            || address.length > u32::MAX as u64 + 8
            || address
                .offset
                .checked_add(address.length)
                .is_none_or(|end| end > file_len)
        {
            return Err(invalid("invalid WAL source address"));
        }
        self.file.seek(SeekFrom::Start(address.offset))?;
        let mut offset = address.offset;
        // The address bounds allocation and disallows a frame consuming its
        // neighbor even if its length prefix has been corrupted.
        let mut bounded = (&mut self.file).take(address.length);
        let FrameRead::Frame(bytes) =
            read_frame_limited(&mut bounded, &mut offset, address.length - 8)?
        else {
            return Err(invalid("truncated referenced WAL source"));
        };
        if offset != address.offset + address.length
            || sha256::digest(&bytes).as_slice() != address.sha256
        {
            return Err(invalid("WAL source content address mismatch"));
        }
        let blob = SourceWalBlob::decode(bytes.as_slice()).map_err(io::Error::other)?;
        validate(&blob)?;
        Ok(blob)
    }

    pub(super) fn restore(&mut self, batch: &mut LoggedAddDocuments) -> io::Result<()> {
        if batch.source_references.len() != batch.documents.len() {
            return Err(invalid("WAL source reference count differs from documents"));
        }
        for (document, reference) in batch.documents.iter_mut().zip(&batch.source_references) {
            if document.original_source.is_some() {
                return Err(invalid(
                    "WAL document has both inline and referenced source",
                ));
            }
            match (&reference.descriptor, &reference.source) {
                (None, None) if document.source_chunk_ordinal.is_none() => continue,
                (Some(descriptor), Some(source)) => {
                    let Some(source_wal_blob::Value::Descriptor(descriptor_set)) =
                        self.blob(descriptor)?.value
                    else {
                        return Err(invalid("WAL descriptor address names a source"));
                    };
                    let Some(source_wal_blob::Value::Source(source)) = self.blob(source)?.value
                    else {
                        return Err(invalid("WAL source address names a descriptor"));
                    };
                    if sha256::digest(&descriptor_set).as_slice() != source.descriptor_sha256 {
                        return Err(invalid("WAL source descriptor mismatch"));
                    }
                    document.original_source = Some(ProtobufSource {
                        descriptor_set,
                        message_type: source.message_type,
                        payload: source.payload,
                    });
                }
                _ => return Err(invalid("incomplete WAL source reference")),
            }
        }
        batch.source_references.clear();
        Ok(())
    }
}
