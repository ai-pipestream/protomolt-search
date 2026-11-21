//! Structural checks on a set manifest before any artifact is opened.

use super::*;

pub(super) fn validate_manifest(manifest: &SegmentSetManifest) -> Result<(), String> {
    validate_format(manifest)?;
    if let Some(binding) = &manifest.binding {
        binding.decode()?;
    }
    if let Some(owner) = &manifest.source_owner {
        owner.decode()?;
    }
    if let Some(declaration) = &manifest.generation_declaration {
        declaration.decode()?;
    }
    let mut ids = BTreeSet::new();
    let mut previous_end = None;
    for segment in &manifest.segments {
        validate_segment_id(&segment.segment_id)?;
        if !ids.insert(&segment.segment_id) {
            return Err(format!("duplicate segment id {:?}", segment.segment_id));
        }
        if segment.rows == 0 || segment.live_rows > segment.rows {
            return Err(format!(
                "segment {:?} has invalid row counts",
                segment.segment_id
            ));
        }
        if let Some(end) = previous_end {
            if segment.base_label < end {
                return Err(format!(
                    "segment {:?} overlaps the preceding stable-id range",
                    segment.segment_id
                ));
            }
        }
        previous_end = Some(segment.end_label_exclusive()?);
        validate_segment_artifacts(segment)?;
    }
    Ok(())
}

/// Only the current format reads. Older catalogs refuse here, by
/// version number, before any per-segment file is parsed: a format-5
/// segment record has no colocation proof to default.
fn validate_format(manifest: &SegmentSetManifest) -> Result<(), String> {
    validate_format_number(manifest.format)
}

/// The format refusal itself, shared by the manifest probe below.
pub(super) fn validate_format_number(format: u32) -> Result<(), String> {
    if format != SET_FORMAT {
        return Err(format!(
            "segment set format {format} is unsupported; this build reads only format \
             {SET_FORMAT}, so rebuild the catalog through the reshard tool"
        ));
    }
    Ok(())
}

/// The one field of a set manifest read before the manifest itself:
/// deserializing a pre-6 manifest whole would fail on segment fields
/// it predates, masking the version refusal with a serde error.
#[derive(Deserialize)]
struct SetFormatProbe {
    format: u32,
}

/// Refuse `bytes` by format number before a caller deserializes the
/// manifest they carry.
pub(super) fn probe_set_format(bytes: &[u8]) -> Result<(), String> {
    let probe: SetFormatProbe = serde_json::from_slice(bytes)
        .map_err(|error| format!("parse segment set format: {error}"))?;
    validate_format_number(probe.format)
}

/// A documents-only segment records no vector and no exact rows (both
/// absent, or both present); every recorded artifact is one relative file
/// with a SHA-256.
fn validate_segment_artifacts(segment: &SegmentMetadata) -> Result<(), String> {
    let has_vectors = !segment.vector.file.is_empty();
    if has_vectors != !segment.exact_vectors.file.is_empty() {
        return Err(format!(
            "segment {:?} records a vector image without exact rows, or the reverse",
            segment.segment_id
        ));
    }
    let mut artifacts: Vec<&SegmentArtifact> = vec![&segment.bm25, &segment.live_docs];
    if has_vectors {
        artifacts.push(&segment.vector);
        artifacts.push(&segment.exact_vectors);
    }
    if let Some(artifact) = &segment.multi_res {
        if !has_vectors {
            return Err("segment retains vector tiers without vector rows".into());
        }
        artifacts.push(artifact);
    }
    for artifact in artifacts {
        validate_relative_file(&artifact.file)?;
        if artifact.sha256.len() != 64
            || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(format!("artifact {:?} has invalid SHA-256", artifact.file));
        }
    }
    Ok(())
}

pub(super) fn validate_relative_file(file: &str) -> Result<(), String> {
    let path = Path::new(file);
    let mut components = path.components();
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(format!(
            "segment artifact path {file:?} is not one relative file"
        ));
    }
    Ok(())
}

pub(super) fn validate_segment_id(segment_id: &str) -> Result<(), String> {
    if segment_id.is_empty()
        || !segment_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!("invalid segment id {segment_id:?}"));
    }
    Ok(())
}
