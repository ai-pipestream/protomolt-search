//! Original protobuf bytes, interned independently of projected chunk rows.
//! The archive can be embedded in an index section; its blob area is borrowed
//! by readers so opening it does not materialize every original in memory.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use prost::Message;

use crate::pb::storage::{SourceArchiveIndex, SourceBlob, SourceIdentity, SourceRecord, SourceRow};
use crate::pb::{DocumentIdentity, ProtobufSource};

const MAGIC: &[u8; 8] = b"PMSOURCE";
const HEADER_BYTES: usize = 48;
const FORMAT_VERSION: u32 = 3;

mod lookup;
use lookup::{build_lookup, validate_lookup, IdentityLink};

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub fn validate_identity(identity: &DocumentIdentity, ordinal: Option<u32>) -> io::Result<()> {
    if identity.document_key.is_empty()
        || identity.document_key.len() > 16 * 1024
        || identity.version == 0
    {
        return Err(invalid(
            "identity requires a 1 to 16384 byte document key and a positive version",
        ));
    }
    if identity.chunk_ordinal != ordinal {
        return Err(invalid("identity and source chunk ordinals differ"));
    }
    Ok(())
}

fn row_identity(
    rows: &[SourceRow],
    identities: &[SourceIdentity],
    row: u32,
) -> Option<DocumentIdentity> {
    let row = rows.get(row as usize)?;
    let identity = identities.get(row.identity.checked_sub(1)? as usize)?;
    Some(DocumentIdentity {
        document_key: identity.document_key.clone(),
        version: identity.version,
        chunk_ordinal: row.chunk_ordinal,
    })
}

mod archive;
mod identity;
mod pages;
mod reader;
#[cfg(test)]
mod tests;

pub use archive::SourceArchive;
pub use identity::IdentitySnapshot;
use identity::IdentitySnapshotData;
use pages::{ArchiveBlob, SharedIdentity, SharedPages};
pub use reader::SourceArchiveReader;
