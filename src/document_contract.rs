//! Receiver agreement for document fields older protobuf decoders can discard.

use crate::pb::AddDocumentsRequest;

/// Version 1 retains typed maps, unsigned values, source and identity.
/// Version 2 also acknowledges derived fingerprints at tag 29.
pub const VERSION: u32 = 2;

/// A legacy peer's row count does not acknowledge these values.
pub fn required_version(document: &AddDocumentsRequest) -> u32 {
    if !document.derived_fingerprint.is_empty() {
        return 2;
    }
    if !document.map_integers.is_empty()
        || !document.map_unsigned_integers.is_empty()
        || !document.unsigned_integers.is_empty()
        || document.original_source.is_some()
        || document.source_chunk_ordinal.is_some()
        || document.identity.is_some()
    {
        1
    } else {
        0
    }
}

pub fn require_supported(advertised: u32, required: u32) -> Result<(), String> {
    if advertised < required {
        Err(format!("receiver document contract version {advertised} cannot acknowledge required version {required}; upgrade the receiver"))
    } else {
        Ok(())
    }
}
