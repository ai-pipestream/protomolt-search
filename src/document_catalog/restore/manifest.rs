//! Read and bind the bundle's completion manifest.
use super::input::read_bounded;
use super::*;

// Count the repeated envelope records before prost allocates them. Unknown
// fields or noncanonical encodings are refused by the full roundtrip below.
fn envelope_budget(bytes: &[u8], maximum: u32) -> Result<(), Status> {
    fn varint(bytes: &mut &[u8]) -> Result<u64, Status> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let (first, rest) = bytes
                .split_first()
                .ok_or_else(|| damaged("truncated manifest varint"))?;
            *bytes = rest;
            if shift == 63 && *first > 1 {
                return Err(damaged("manifest varint overflow"));
            }
            value |= u64::from(first & 0x7f) << shift;
            if first & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(damaged("manifest varint overflow"))
    }
    fn walk(
        mut bytes: &[u8],
        maximum: u32,
        envelope: bool,
        counts: &mut [u32; 3],
    ) -> Result<(), Status> {
        while !bytes.is_empty() {
            let tag = varint(&mut bytes)?;
            if tag >> 3 == 0 {
                return Err(damaged("zero manifest field number"));
            }
            match tag & 7 {
                0 => {
                    varint(&mut bytes)?;
                }
                1 | 5 => {
                    let length = if tag & 7 == 1 { 8 } else { 4 };
                    bytes = bytes
                        .get(length..)
                        .ok_or_else(|| damaged("truncated manifest field"))?;
                }
                2 => {
                    let length = usize::try_from(varint(&mut bytes)?).map_err(|_| exhausted())?;
                    let payload = bytes
                        .get(..length)
                        .ok_or_else(|| damaged("truncated manifest field"))?;
                    bytes = &bytes[length..];
                    if envelope && tag >> 3 == 2 {
                        // Repeated encodings of the singular source field merge
                        // in protobuf. Count their combined checkpoint indexes.
                        walk(payload, maximum, false, counts)?;
                    }
                    let slot = match (envelope, tag >> 3) {
                        (true, 3) => Some(0),
                        (true, 4) => Some(1),
                        (false, 3) => Some(2),
                        _ => None,
                    };
                    if let Some(slot) = slot {
                        counts[slot] = counts[slot].checked_add(1).ok_or_else(exhausted)?;
                        if counts[slot] > maximum {
                            return Err(exhausted());
                        }
                    }
                }
                _ => return Err(damaged("unsupported manifest wire type")),
            }
        }
        Ok(())
    }
    walk(bytes, maximum, true, &mut [0; 3])
}

pub(super) fn manifest(
    root: &Directory,
    request: &SourceRestoreRequest,
) -> Result<(SourceBackupManifest, Vec<u8>), Status> {
    let limits = request.limits.as_ref().expect("validated limits");
    let bytes = read_bounded(root, COMPLETION, limits.metadata_bytes)?;
    envelope_budget(&bytes, limits.max_files)?;
    let manifest: SourceBackupManifest = decode(&bytes)?;
    if manifest.format_version != 1 || manifest.encode_to_vec() != bytes {
        return Err(damaged("unsupported or noncanonical completion manifest"));
    }
    let mut unsigned = manifest.clone();
    let digest = std::mem::take(&mut unsigned.manifest_sha256);
    if digest != request.expected_manifest_sha256
        || turbo_sha256::sha256(&unsigned.encode_to_vec()).0.as_slice() != digest
    {
        return Err(damaged(
            "completion digest differs from the owner's expectation",
        ));
    }
    let source = manifest
        .source
        .as_ref()
        .ok_or_else(|| damaged("completion has no source checkpoint"))?;
    let header = source
        .header
        .as_ref()
        .ok_or_else(|| damaged("checkpoint has no header"))?;
    validate_current_header(header)?;
    if source.format_version != 1
        || header.collection != request.collection
        || header.history_id != request.history_id
    {
        return Err(damaged(
            "checkpoint differs from the expected collection or history",
        ));
    }
    if source.sha256.len() != 32
        || source.bytes == 0
        || source.records == 0
        || manifest.indexes.len() != source.indexes.len()
        || manifest.artifacts.is_empty()
    {
        return Err(damaged("invalid source checkpoint or index inventory"));
    }
    Ok((manifest, bytes))
}
