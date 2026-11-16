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

/// The format number must match what the manifest declares.
fn validate_format(manifest: &SegmentSetManifest) -> Result<(), String> {
    if !(BASE_SET_FORMAT..=RETAINED_SET_FORMAT).contains(&manifest.format)
        || (manifest.format == BASE_SET_FORMAT && manifest.binding.is_some())
        || (manifest.format == SET_FORMAT && manifest.binding.is_none())
        || (manifest.format < SOURCE_SET_FORMAT && manifest.source_owner.is_some())
        || (manifest.format == SOURCE_SET_FORMAT && manifest.source_owner.is_none())
        || (manifest.format < RETAINED_SET_FORMAT
            && ((manifest.format == DECLARED_SET_FORMAT)
                != manifest.generation_declaration.is_some()))
        || (manifest.format < RETAINED_SET_FORMAT
            && manifest
                .segments
                .iter()
                .any(|segment| segment.multi_res.is_some()))
    {
        return Err(format!(
            "segment set format {} or binding declaration is unsupported; expected format \
             {BASE_SET_FORMAT} without a binding, {SET_FORMAT} with a binding, or {SOURCE_SET_FORMAT} with a source owner, or {DECLARED_SET_FORMAT} with a generation declaration",
            manifest.format
        ));
    }
    Ok(())
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
