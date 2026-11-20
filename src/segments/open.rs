//! Open and verify the segments a set manifest names.

use super::*;

/// The one scoring fingerprint and the one analysis table a set may hold.
#[derive(Default)]
struct SetFingerprints<'m> {
    scoring: Option<&'m str>,
    analysis: Option<&'m [u64]>,
}

impl<'m> SetFingerprints<'m> {
    fn admit_scoring(&mut self, metadata: &'m SegmentMetadata) -> Result<(), String> {
        match self.scoring {
            Some(known) if known != metadata.scoring_fingerprint => {
                return Err("segment set mixes vector scoring fingerprints".into())
            }
            None => self.scoring = Some(&metadata.scoring_fingerprint),
            _ => {}
        }
        Ok(())
    }

    fn admit_analysis(&mut self, metadata: &'m SegmentMetadata) -> Result<(), String> {
        match self.analysis {
            Some(known) if known != metadata.analysis_fingerprints => {
                return Err("segment set mixes analysis fingerprints".into())
            }
            None => self.analysis = Some(&metadata.analysis_fingerprints),
            _ => {}
        }
        Ok(())
    }
}

/// The row counts of one segment's opened artifacts.
struct ArtifactRows {
    vector: usize,
    exact: usize,
    bm25: usize,
}

impl OpenedSegmentSet {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        Self::open_with(root, VectorLoad::default())
    }

    pub fn open_with(root: impl Into<PathBuf>, load: VectorLoad) -> Result<Self, String> {
        let root = root.into();
        let path = root.join(SET_FILE);
        let manifest = if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read segment set {}: {error}", path.display()))?;
            probe_set_format(&bytes)?;
            serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse segment set {}: {error}", path.display()))?
        } else {
            SegmentSetManifest::default()
        };
        Self::open_manifest(root, manifest, load)
    }

    pub(super) fn open_manifest(
        root: PathBuf,
        manifest: SegmentSetManifest,
        load: VectorLoad,
    ) -> Result<Self, String> {
        Self::open_manifest_reusing(root, manifest, load, None)
    }

    /// Open `manifest`, taking every segment whose metadata is unchanged
    /// from `reuse` as it is (open, verified, its images prepared). A bitmap-only
    /// change shares those images and verifies the new bitmap. Other changes
    /// open and verify the new images. A publish adds one segment to a set of
    /// hundreds, so reuse keeps the hashing a publish does independent of
    /// the seal count: only what changed is hashed. A node open passes no
    /// `reuse` and verifies all.
    pub(super) fn open_manifest_reusing(
        root: PathBuf,
        manifest: SegmentSetManifest,
        load: VectorLoad,
        reuse: Option<&OpenedSegmentSet>,
    ) -> Result<Self, String> {
        validate_manifest(&manifest)?;
        let mut segments = Vec::with_capacity(manifest.segments.len());
        let mut fingerprints = SetFingerprints::default();
        for metadata in &manifest.segments {
            let held = reuse.and_then(|set| {
                set.segments
                    .iter()
                    .find(|segment| same_segment_images(&segment.metadata, metadata))
            });
            if let Some(held) = held {
                if held.vector.is_some() {
                    fingerprints.admit_scoring(metadata)?;
                }
                fingerprints.admit_analysis(metadata)?;
                segments.push(reuse_segment(&root, metadata, held)?);
                continue;
            }
            let opened = open_segment(&root, metadata, load)?;
            if opened.vector.is_some() {
                fingerprints.admit_scoring(metadata)?;
            }
            fingerprints.admit_analysis(metadata)?;
            segments.push(Arc::new(opened));
        }
        let binding = generation_binding(&manifest, &segments)?;
        let declaration = checked_declaration(&manifest, &segments)?;
        Ok(Self {
            root,
            binding,
            declaration,
            manifest,
            segments,
        })
    }
}

/// The binding the manifest declares, or else the one every segment agrees on.
fn generation_binding(
    manifest: &SegmentSetManifest,
    segments: &[Arc<OpenedSegment>],
) -> Result<Option<StoredBinding>, String> {
    let declared = manifest
        .binding
        .as_ref()
        .map(SegmentBinding::decode)
        .transpose()?;
    let mut held = declared.clone().map(Some);
    for segment in segments {
        let next = segment.bm25.binding().cloned();
        match &held {
            Some(expected) if expected != &next => {
                return Err(
                    "segment set mixes mapped bindings or disagrees with its generation binding"
                        .into(),
                )
            }
            None => held = Some(next),
            _ => {}
        }
    }
    Ok(declared.or_else(|| held.flatten()))
}

/// The manifest's generation declaration, checked against every segment.
fn checked_declaration(
    manifest: &SegmentSetManifest,
    segments: &[Arc<OpenedSegment>],
) -> Result<Option<crate::pb::storage::IndexGenerationDeclaration>, String> {
    let declaration = manifest
        .generation_declaration
        .as_ref()
        .map(SegmentGenerationDeclaration::decode)
        .transpose()?;
    if let Some(declaration) = &declaration {
        for segment in segments {
            generation::check_reader(declaration, &segment.bm25, segment.vector.as_deref())?;
        }
    }
    Ok(declaration)
}

/// Take `held` as it is when its metadata is unchanged; otherwise share its
/// images and verify the new bitmap.
fn reuse_segment(
    root: &Path,
    metadata: &SegmentMetadata,
    held: &Arc<OpenedSegment>,
) -> Result<Arc<OpenedSegment>, String> {
    if held.metadata == *metadata {
        return Ok(Arc::clone(held));
    }
    let directory = root.join("segments").join(&metadata.segment_id);
    verify_artifact(&directory, &metadata.live_docs)?;
    let live_docs = LiveDocs::open(&directory.join(&metadata.live_docs.file))
        .map_err(|error| format!("open retired segment bitmap: {error}"))?;
    if live_docs.requires_retained_vectors() && metadata.multi_res.is_none() {
        return Err(format!(
            "segment {:?} requires a retained vector artifact",
            metadata.segment_id
        ));
    }
    if live_docs.persisted_rows() != metadata.rows
        || metadata.rows.checked_sub(live_docs.deleted_count()) != Some(metadata.live_rows)
    {
        return Err("retired segment bitmap differs from manifest row counts".into());
    }
    Ok(Arc::new(OpenedSegment {
        metadata: metadata.clone(),
        vector: held.vector.clone(),
        exact: held.exact.clone(),
        retained: Arc::clone(&held.retained),
        bm25: Arc::clone(&held.bm25),
        live_docs,
    }))
}

/// Open and verify every artifact of a segment no held set already has open.
fn open_segment(
    root: &Path,
    metadata: &SegmentMetadata,
    load: VectorLoad,
) -> Result<OpenedSegment, String> {
    let directory = root.join("segments").join(&metadata.segment_id);
    let has_vectors = verify_images(&directory, metadata)?;
    let vector = open_vector(&directory, metadata, load, has_vectors)?;
    let exact = open_exact(&directory, metadata, vector.as_ref(), has_vectors)?;
    let retained = open_retained(&directory, metadata)?;
    let exact_rows = exact.as_ref().map_or(0, |store| store.len());
    let bm25 = Bm25Reader::open(&directory.join(&metadata.bm25.file))
        .map_err(|error| format!("open BM25 segment {:?}: {error}", metadata.segment_id))?;
    let live_docs = LiveDocs::open(&directory.join(&metadata.live_docs.file))
        .map_err(|error| format!("open live-doc segment {:?}: {error}", metadata.segment_id))?;
    if live_docs.requires_retained_vectors() && metadata.multi_res.is_none() {
        return Err(format!(
            "segment {:?} requires a retained vector artifact",
            metadata.segment_id
        ));
    }
    let counted = ArtifactRows {
        vector: vector.as_ref().map_or(0, VectorIndex::len),
        exact: exact_rows,
        bm25: bm25.next_doc_id() as usize,
    };
    check_alignment(metadata, has_vectors, &counted, &live_docs)?;
    check_statistics(metadata, &bm25, &live_docs)?;
    if let Some(vector) = &vector {
        check_scoring_contract(metadata, vector)?;
    }
    Ok(OpenedSegment {
        metadata: metadata.clone(),
        vector: vector.map(Arc::new),
        exact,
        retained: Arc::new(retained),
        bm25: Arc::new(bm25),
        live_docs,
    })
}

/// Hash every image the manifest records; whether the segment has vectors.
fn verify_images(directory: &Path, metadata: &SegmentMetadata) -> Result<bool, String> {
    let has_vectors = !metadata.vector.file.is_empty();
    if has_vectors {
        verify_artifact(directory, &metadata.vector)?;
        verify_artifact(directory, &metadata.exact_vectors)?;
    } else if !metadata.exact_vectors.file.is_empty() {
        return Err(format!(
            "segment {:?} has exact rows but no vector artifact",
            metadata.segment_id
        ));
    }
    verify_artifact(directory, &metadata.bm25)?;
    verify_artifact(directory, &metadata.live_docs)?;
    Ok(has_vectors)
}

fn open_vector(
    directory: &Path,
    metadata: &SegmentMetadata,
    load: VectorLoad,
    has_vectors: bool,
) -> Result<Option<VectorIndex>, String> {
    if !has_vectors {
        return Ok(None);
    }
    let image = directory.join(&metadata.vector.file);
    let mut vector = match load {
        VectorLoad::Mapped => VectorIndex::load_mapped(&metadata.backend_kind, &image),
        VectorLoad::Heap => VectorIndex::load(&metadata.backend_kind, &image),
    }
    .map_err(|error| format!("open vector segment {:?}: {error}", metadata.segment_id))?;
    vector
        .prepare()
        .map_err(|error| format!("prepare vector segment {:?}: {error}", metadata.segment_id))?;
    Ok(Some(vector))
}

fn open_exact(
    directory: &Path,
    metadata: &SegmentMetadata,
    vector: Option<&VectorIndex>,
    has_vectors: bool,
) -> Result<Option<Arc<ExactVectorStore>>, String> {
    if !has_vectors {
        return Ok(None);
    }
    let exact = ExactVectorStore::open(&directory.join(&metadata.exact_vectors.file))
        .map_err(|error| format!("open exact segment {:?}: {error}", metadata.segment_id))?;
    if exact.dim() != vector.and_then(VectorIndex::dim_opt) {
        return Err(format!(
            "segment {:?} exact-vector dimension differs from its provider",
            metadata.segment_id
        ));
    }
    Ok(Some(Arc::new(exact)))
}

fn open_retained(
    directory: &Path,
    metadata: &SegmentMetadata,
) -> Result<crate::multi_res::MultiResVectors, String> {
    match &metadata.multi_res {
        Some(artifact) => {
            verify_artifact(directory, artifact)?;
            crate::multi_res::MultiResVectors::open(
                &directory.join(&artifact.file),
                metadata.rows,
                &artifact.sha256,
            )
            .map_err(|error| format!("open retained vector segment: {error}"))
        }
        None => Ok(crate::multi_res::MultiResVectors::default()),
    }
}

/// Row alignment: every artifact a segment has covers the same rows; a
/// documents-only segment has no vector rows at all.
fn check_alignment(
    metadata: &SegmentMetadata,
    has_vectors: bool,
    counted: &ArtifactRows,
    live_docs: &LiveDocs,
) -> Result<(), String> {
    let rows = usize::try_from(metadata.rows)
        .map_err(|_| format!("segment {:?} rows do not fit usize", metadata.segment_id))?;
    let aligned = |n: usize| n == 0 || n == rows;
    if rows == 0
        || !aligned(counted.vector)
        || (has_vectors && counted.vector != rows)
        || !aligned(counted.exact)
        || counted.exact != counted.vector
        || !aligned(counted.bm25)
        || (counted.vector == 0 && counted.bm25 == 0)
        || live_docs.persisted_rows() != metadata.rows
    {
        return Err(format!(
            "segment {:?} is not aligned: manifest rows {}, vector {}, exact {}, BM25 {}, live-doc {}",
            metadata.segment_id,
            metadata.rows,
            counted.vector,
            counted.exact,
            counted.bm25,
            live_docs.persisted_rows()
        ));
    }
    Ok(())
}

fn check_statistics(
    metadata: &SegmentMetadata,
    bm25: &Bm25Reader,
    live_docs: &LiveDocs,
) -> Result<(), String> {
    let live_rows = metadata
        .rows
        .checked_sub(live_docs.deleted_count())
        .ok_or_else(|| {
            format!(
                "segment {:?} deletes exceed its physical rows",
                metadata.segment_id
            )
        })?;
    if metadata.live_rows != live_rows
        || metadata.document_count != Bm25Index::doc_count(bm25)
        || metadata.total_document_length != Bm25Index::total_doc_length(bm25)
    {
        return Err(format!(
            "segment {:?} manifest statistics differ from its artifacts",
            metadata.segment_id
        ));
    }
    Ok(())
}

fn check_scoring_contract(metadata: &SegmentMetadata, vector: &VectorIndex) -> Result<(), String> {
    let descriptor = vector.descriptor();
    if descriptor.scoring_fingerprint != metadata.scoring_fingerprint
        || descriptor.score_direction != ScoreDirection::HigherIsBetter
        || !admitted_quality_contract(descriptor.quality_contract)
    {
        return Err(format!(
            "segment {:?} vector scoring contract differs from its aligned manifest or is neither exhaustive nor a configured ANN traversal",
            metadata.segment_id
        ));
    }
    Ok(())
}

fn same_segment_images(left: &SegmentMetadata, right: &SegmentMetadata) -> bool {
    if left.segment_id != right.segment_id {
        return false;
    }
    let mut normalized = left.clone();
    normalized.live_docs = right.live_docs.clone();
    normalized.live_rows = right.live_rows;
    normalized == *right
}
