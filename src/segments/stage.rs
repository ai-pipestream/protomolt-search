//! Stage immutable segment directories before a manifest names them.

use super::*;

/// Stage `sources` under `root/segments/` — copied, hashed, opened,
/// verified, fsynced, and renamed into their final directories — WITHOUT
/// publishing them: they join no manifest until a caller commits one.
/// A compaction stages its dense outputs this way while the live set
/// keeps serving. On any failure every directory
/// this call created is removed. A source whose directory already exists
/// refuses by name rather than adopting it.
pub fn stage_segments(
    root: &Path,
    sources: Vec<SegmentSource<'_>>,
) -> Result<Vec<SegmentMetadata>, String> {
    std::fs::create_dir_all(root.join("segments"))
        .map_err(|error| format!("mkdir segment root {}: {error}", root.display()))?;
    let mut staged = Vec::with_capacity(sources.len());
    for source in sources {
        match stage_segment(root, source) {
            Ok(metadata) => staged.push(metadata),
            Err(error) => {
                cleanup_staged(root, &staged);
                return Err(error);
            }
        }
    }
    Ok(staged)
}

/// Write a set manifest to `path` atomically: the copy a compaction
/// cutover keeps for rollback beside the live one.
pub fn write_manifest_file(path: &Path, manifest: &SegmentSetManifest) -> Result<(), String> {
    if path.file_name().is_some_and(|name| name == SET_FILE) {
        let held = SegmentCatalog::read_manifest(path.parent().unwrap_or(Path::new(".")))?;
        if manifest.source_owner.is_some()
            || held.as_ref().is_some_and(|m| m.source_owner.is_some())
        {
            return Err("source-managed index manifests require source publication".into());
        }
    }
    write_json_atomic(path, manifest)
}

/// Remove the directories of `segments` under `root`: the staged outputs
/// of a compaction that did not commit, or the inputs one replaced.
pub fn remove_segment_dirs(root: &Path, segments: &[SegmentMetadata]) {
    cleanup_staged(root, segments)
}

pub(super) fn cleanup_staged(root: &Path, segments: &[SegmentMetadata]) {
    // A rename can succeed before directory fsync fails. Never delete files
    // named by that manifest, and retain them if the manifest cannot be read.
    let manifest = match SegmentCatalog::read_manifest(root) {
        Ok(value) => value,
        Err(_) => return,
    };
    for segment in segments {
        if manifest.as_ref().is_some_and(|m| {
            m.segments
                .iter()
                .any(|s| s.segment_id == segment.segment_id)
        }) {
            continue;
        }
        let _ = std::fs::remove_dir_all(root.join("segments").join(&segment.segment_id));
    }
}

pub(super) fn verify_retry_source(
    root: &Path,
    source: &SegmentSource<'_>,
    held: &SegmentMetadata,
) -> Result<(), String> {
    let backend = if source.vector_path.is_some() {
        source.backend_kind
    } else {
        ""
    };
    if source.generation != held.generation
        || source.base_label != held.base_label
        || backend != held.backend_kind
        || source.parents_colocated != held.parents_colocated
    {
        return Err("segment retry differs from the published generation, range or backend".into());
    }
    let expected_partition = source.partition_column.and_then(|column| {
        held.summary
            .as_ref()?
            .int_columns
            .iter()
            .find(|summary| summary.name == column && summary.present > 0)
            .map(|summary| PartitionRange {
                column: column.into(),
                lo: summary.min,
                hi: summary.max,
            })
    });
    if held.summary.as_ref().and_then(|s| s.partition.clone()) != expected_partition {
        return Err("segment retry differs from the published partition contract".into());
    }
    let directory = SegmentCatalog::segment_dir(root, &held.segment_id);
    match (source.multi_res_path, held.multi_res.as_ref()) {
        (Some(path), Some(artifact)) => {
            let (bytes, sha256) = digest_file(path)?;
            if bytes != artifact.bytes || sha256 != artifact.sha256 {
                return Err("segment retry differs from retained vector artifact".into());
            }
            verify_artifact(&directory, artifact)?;
        }
        (None, None) => {}
        _ => return Err("segment retry differs from retained vector declaration".into()),
    }
    for (source, artifact) in [
        (source.vector_path, &held.vector),
        (source.exact_vector_path, &held.exact_vectors),
        (Some(source.bm25_path), &held.bm25),
        (Some(source.live_docs_path), &held.live_docs),
    ] {
        match source {
            Some(path) if !artifact.file.is_empty() => {
                let (bytes, sha256) = digest_file(path)?;
                if bytes != artifact.bytes || sha256 != artifact.sha256 {
                    return Err("segment retry content differs from its published artifact".into());
                }
                verify_artifact(&directory, artifact)?;
            }
            None if artifact.file.is_empty()
                && artifact.bytes == 0
                && artifact.sha256.is_empty() => {}
            _ => return Err("segment retry differs from the published artifact shape".into()),
        }
    }
    Ok(())
}

pub(super) fn stage_segment(
    root: &Path,
    source: SegmentSource<'_>,
) -> Result<SegmentMetadata, String> {
    validate_segment_id(source.segment_id)?;
    let final_dir = root.join("segments").join(source.segment_id);
    if final_dir.exists() {
        return Err(format!("segment {:?} already exists", source.segment_id));
    }
    let temp_dir = root.join("segments").join(format!(
        ".tmp-{}-{}-{}",
        source.segment_id,
        std::process::id(),
        source.generation
    ));
    // Maintenance stages outside the publication fence. Another build may
    // already own this directory; only exclusive creation permits cleanup.
    std::fs::create_dir(&temp_dir).map_err(|error| {
        format!(
            "create private stage {}: {error}; existing stages must be retained for their owner",
            temp_dir.display()
        )
    })?;
    let result = stage_into(root, &source, &temp_dir, &final_dir);
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
    result
}

/// The artifact records of the copies in a private stage.
struct StagedArtifacts {
    vector: SegmentArtifact,
    exact: SegmentArtifact,
    bm25: SegmentArtifact,
    live_docs: SegmentArtifact,
}

/// The staged copies, opened and verified to cover the same `rows`.
struct StagedImages {
    vector: Option<VectorIndex>,
    bm25: Bm25Reader,
    live_docs: LiveDocs,
    rows: usize,
}

fn stage_into(
    root: &Path,
    source: &SegmentSource<'_>,
    temp_dir: &Path,
    final_dir: &Path,
) -> Result<SegmentMetadata, String> {
    let artifacts = copy_source_artifacts(source, temp_dir)?;
    let images = open_staged_images(source, temp_dir)?;
    let multi_res = stage_retained(source, temp_dir, &images)?;
    let metadata = staged_metadata(source, artifacts, multi_res, &images)?;
    write_json_atomic(&temp_dir.join(SEGMENT_META_FILE), &metadata)?;
    std::fs::File::open(temp_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync stage {}: {error}", temp_dir.display()))?;
    std::fs::rename(temp_dir, final_dir).map_err(|error| {
        format!(
            "publish segment stage {} -> {}: {error}",
            temp_dir.display(),
            final_dir.display()
        )
    })?;
    std::fs::File::open(root.join("segments"))
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync segment directory: {error}"))?;
    Ok(metadata)
}

/// A documents-only segment has no vector rows; the exact rows go with the
/// vector rows.
fn copy_source_artifacts(
    source: &SegmentSource<'_>,
    temp_dir: &Path,
) -> Result<StagedArtifacts, String> {
    let (vector, exact) = match (source.vector_path, source.exact_vector_path) {
        (Some(vector_path), Some(exact_path)) => (
            copy_artifact(vector_path, temp_dir, "vector.index")?,
            copy_artifact(exact_path, temp_dir, "vectors.f32")?,
        ),
        (None, None) => (absent_artifact(), absent_artifact()),
        _ => {
            return Err(
                "a segment carries its vector image and its exact rows together, or neither"
                    .to_string(),
            )
        }
    };
    Ok(StagedArtifacts {
        vector,
        exact,
        bm25: copy_artifact(source.bm25_path, temp_dir, "documents.bm25")?,
        live_docs: copy_artifact(source.live_docs_path, temp_dir, "live-docs.bin")?,
    })
}

fn open_staged_images(source: &SegmentSource<'_>, temp_dir: &Path) -> Result<StagedImages, String> {
    let vector = if source.vector_path.is_some() {
        let mut vector = VectorIndex::load(source.backend_kind, &temp_dir.join("vector.index"))
            .map_err(|error| format!("open staged vector: {error}"))?;
        vector
            .prepare()
            .map_err(|error| format!("prepare staged vector: {error}"))?;
        Some(vector)
    } else {
        None
    };
    let exact_rows = if source.exact_vector_path.is_some() {
        let exact = ExactVectorStore::open(&temp_dir.join("vectors.f32"))
            .map_err(|error| format!("open staged exact vectors: {error}"))?;
        exact
            .verify_payload()
            .map_err(|error| format!("verify staged exact vectors: {error}"))?;
        if exact.dim() != vector.as_ref().and_then(VectorIndex::dim_opt) {
            return Err("staged segment exact-vector dimension differs from its provider".into());
        }
        exact.len()
    } else {
        0
    };
    let bm25 = Bm25Reader::open(&temp_dir.join("documents.bm25"))
        .map_err(|error| format!("open staged BM25: {error}"))?;
    bm25.verify_integrity()
        .map_err(|error| format!("verify staged BM25: {error}"))?;
    let live_docs = LiveDocs::open(&temp_dir.join("live-docs.bin"))
        .map_err(|error| format!("open staged live docs: {error}"))?;
    let vector_rows = vector.as_ref().map_or(0, VectorIndex::len);
    let bm25_rows = bm25.next_doc_id() as usize;
    let rows = vector_rows.max(bm25_rows);
    let aligned = |n: usize| n == 0 || n == rows;
    if rows == 0
        || !aligned(vector_rows)
        || (vector.is_some() && vector_rows != rows)
        || exact_rows != vector_rows
        || !aligned(bm25_rows)
        || live_docs.persisted_rows() != rows as u64
    {
        return Err(format!(
            "staged segment is not aligned: vector {vector_rows}, exact {exact_rows}, BM25 \
             {bm25_rows}, live-doc {}",
            live_docs.persisted_rows()
        ));
    }
    Ok(StagedImages {
        vector,
        bm25,
        live_docs,
        rows,
    })
}

fn stage_retained(
    source: &SegmentSource<'_>,
    temp_dir: &Path,
    images: &StagedImages,
) -> Result<Option<SegmentArtifact>, String> {
    source
        .multi_res_path
        .map(|path| {
            if images.vector.is_none() {
                return Err("cannot retain tiers in a documents-only segment".to_string());
            }
            let artifact = copy_artifact(path, temp_dir, crate::multi_res::FILE_NAME)?;
            crate::multi_res::MultiResVectors::open(
                &temp_dir.join(&artifact.file),
                images.rows as u64,
                &artifact.sha256,
            )
            .map_err(|error| format!("verify retained segment vectors: {error}"))?;
            Ok(artifact)
        })
        .transpose()
}

fn staged_metadata(
    source: &SegmentSource<'_>,
    artifacts: StagedArtifacts,
    multi_res: Option<SegmentArtifact>,
    images: &StagedImages,
) -> Result<SegmentMetadata, String> {
    let (backend_kind, scoring_fingerprint) = match &images.vector {
        Some(vector) => {
            let descriptor = vector.descriptor();
            if descriptor.score_direction != ScoreDirection::HigherIsBetter
                || !admitted_quality_contract(descriptor.quality_contract)
            {
                return Err(
                    "aligned segments require a higher-is-better provider that is exhaustive \
                     or a configured ANN traversal"
                        .into(),
                );
            }
            (
                source.backend_kind.to_string(),
                descriptor.scoring_fingerprint,
            )
        }
        None => (String::new(), String::new()),
    };
    let (bm25, rows) = (&images.bm25, images.rows);
    Ok(SegmentMetadata {
        segment_id: source.segment_id.to_string(),
        generation: source.generation,
        base_label: source.base_label,
        rows: rows as u64,
        live_rows: rows as u64 - images.live_docs.deleted_count(),
        backend_kind,
        scoring_fingerprint,
        analysis_fingerprints: (0..bm25.field_count())
            .map(|field| bm25.analysis_fingerprint(field))
            .collect(),
        parents_colocated: source.parents_colocated,
        document_count: Bm25Index::doc_count(bm25),
        total_document_length: Bm25Index::total_doc_length(bm25),
        vector: artifacts.vector,
        exact_vectors: artifacts.exact,
        multi_res,
        bm25: artifacts.bm25,
        live_docs: artifacts.live_docs,
        summary: Some(staged_summary(source, bm25, rows)),
    })
}

fn staged_summary(source: &SegmentSource<'_>, bm25: &Bm25Reader, rows: usize) -> SegmentSummary {
    let mut summary = summarize_columns(bm25, rows as u32);
    summary.partition = source.partition_column.and_then(|column| {
        summary
            .int_columns
            .iter()
            .find(|c| c.name == column && c.present > 0)
            .map(|c| PartitionRange {
                column: column.to_string(),
                lo: c.min,
                hi: c.max,
            })
    });
    summary
}
