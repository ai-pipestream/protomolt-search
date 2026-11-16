//! Seal a built child image as the one-segment catalog its shard serves.

use std::path::{Path, PathBuf};

use crate::live_docs::LiveDocs;
use crate::postings::{Bm25Reader, Bm25Store};
use crate::segments::{SegmentCatalog, SegmentSource};

use super::{ChildImage, ReshardOutput};

/// Seal every child of a split or merge into the catalog its shard serves
/// at `segments_root(vector_path)`. Each image's files move into one
/// segment; the image then names the catalog the way a tree split's
/// segmented child does: `vector_path` is the child's index path,
/// `exact_vector_path` its catalog root, and no loose sidecar remains.
pub(super) fn sealed(
    manifest: &crate::wal::WalManifest,
    generation: u64,
    mut children: Vec<ChildImage>,
) -> Result<ReshardOutput, String> {
    for image in &mut children {
        seal_child_catalog(image, generation, manifest)?;
    }
    Ok(ReshardOutput {
        generation,
        children,
    })
}

fn seal_child_catalog(
    image: &mut ChildImage,
    generation: u64,
    manifest: &crate::wal::WalManifest,
) -> Result<(), String> {
    let root = crate::node::segments_root(&image.vector_path);
    if root.exists() {
        return Err(format!(
            "{} already exists; a reshard writes each child's catalog fresh",
            root.display()
        ));
    }
    let catalog = SegmentCatalog::open(&root)?;
    let rows = image.row_parent_ids.len() as u64;
    if rows > 0 {
        let staged = StagedFiles::write(image, rows, manifest)?;
        let has_vectors = image.num_vectors > 0;
        catalog
            .append(SegmentSource {
                multi_res_path: image.multi_res_path.as_deref(),
                segment_id: &format!("g{generation}-p0"),
                generation,
                base_label: 0,
                backend_kind: &manifest.vector_backend,
                vector_path: has_vectors.then_some(image.vector_path.as_path()),
                exact_vector_path: has_vectors.then_some(image.exact_vector_path.as_path()),
                bm25_path: &staged.bm25,
                live_docs_path: &staged.live,
                partition_column: None,
            })
            .map_err(|error| format!("seal child {}: {error}", root.display()))?;
        if let Some(binding) = Bm25Reader::open(&staged.bm25)
            .map_err(|error| format!("read {}: {error}", staged.bm25.display()))?
            .binding()
        {
            catalog.publish_binding(binding)?;
        }
        staged.remove(image)?;
    } else {
        // An empty child keeps only its empty provider image at the index
        // path: the tail a node adopts with the parent's calibration.
        for path in [Some(&image.exact_vector_path), image.bm25_path.as_ref()]
            .into_iter()
            .flatten()
        {
            remove_if_present(path)?;
        }
    }
    image.exact_vector_path = root;
    image.bm25_path = None;
    image.multi_res_path = None;
    Ok(())
}

/// The files a child's segment is staged from besides its image: its
/// documents (an empty store under the log's tables when the child
/// received none) and its all-live row overlay.
struct StagedFiles {
    bm25: PathBuf,
    live: PathBuf,
}

impl StagedFiles {
    fn write(
        image: &ChildImage,
        rows: u64,
        manifest: &crate::wal::WalManifest,
    ) -> Result<Self, String> {
        let bm25 = match &image.bm25_path {
            Some(path) => path.clone(),
            None => {
                let path = sibling(&image.vector_path, ".documents.bm25");
                empty_store(manifest)?
                    .save(&path)
                    .map_err(|error| format!("write {}: {error}", path.display()))?;
                path
            }
        };
        let live = crate::node::live_docs_sidecar_path(&image.vector_path);
        LiveDocs::default()
            .write(&live, rows)
            .map_err(|error| format!("write {}: {error}", live.display()))?;
        Ok(Self { bm25, live })
    }

    /// The catalog holds copies of every staged file; remove the originals.
    fn remove(self, image: &ChildImage) -> Result<(), String> {
        let retained = image.multi_res_path.as_deref();
        let declaration = retained.map(crate::multi_res_storage::declaration_path);
        let files = [
            Some(image.vector_path.as_path()),
            Some(image.exact_vector_path.as_path()),
            Some(self.bm25.as_path()),
            Some(self.live.as_path()),
            retained,
            declaration.as_deref(),
        ];
        for path in files.into_iter().flatten() {
            remove_if_present(path)?;
        }
        Ok(())
    }
}

fn remove_if_present(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove staged {}: {error}", path.display())),
    }
}

/// A store with no documents declaring the field and column tables and the
/// derived-column declaration the log's manifest records for its shard.
fn empty_store(manifest: &crate::wal::WalManifest) -> Result<Bm25Store, String> {
    let table = manifest.columns.as_ref().ok_or_else(|| {
        "the log's manifest records no column table, so a child without documents has no \
         tables to declare"
            .to_string()
    })?;
    let mut store = Bm25Store::with_fields(&names(&table.fields))
        .with_facets(&names(&table.facets))
        .with_numerics(&names(&table.numerics))
        .with_map_facets(&names(&table.map_facets))
        .with_map_numerics(&names(&table.map_numerics))
        .with_map_integers(&names(&table.map_integers))
        .with_map_unsigned_integers(&names(&table.map_unsigned_integers))
        .with_integers(&names(&table.integers))
        .with_unsigned_integers(&names(&table.unsigned_integers))
        .with_geos(&names(&table.geo));
    store.set_derived(super::child_build::manifest_derived(manifest)?);
    Ok(store)
}

fn names(list: &[String]) -> Vec<&str> {
    list.iter().map(String::as_str).collect()
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
