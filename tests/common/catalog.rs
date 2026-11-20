//! A one-segment catalog over a `.bm25` store a test wrote by hand: the
//! shard a node opens at `index_path`.

use std::path::Path;

use pipestream_search::live_docs::LiveDocs;
use pipestream_search::node::segments_root;
use pipestream_search::postings::Bm25Reader;
use pipestream_search::segments::{SegmentCatalog, SegmentSource};

/// Seal the store saved at `bm25_path` as the only segment of the catalog
/// at `segments_root(index_path)`, carrying the store's mapped binding.
pub fn seal_bm25_file(index_path: &Path, bm25_path: &Path) {
    let reader = Bm25Reader::open(bm25_path).unwrap();
    let mut live = bm25_path.as_os_str().to_owned();
    live.push(".live");
    let live = std::path::PathBuf::from(live);
    LiveDocs::default()
        .write(&live, u64::from(reader.next_doc_id()))
        .unwrap();
    let catalog = SegmentCatalog::open(segments_root(index_path)).unwrap();
    catalog
        .append(SegmentSource {
            multi_res_path: None,
            segment_id: "seg-0",
            generation: 1,
            base_label: 0,
            backend_kind: "",
            vector_path: None,
            exact_vector_path: None,
            bm25_path,
            live_docs_path: &live,
            partition_column: None,
        })
        .unwrap();
    if let Some(binding) = reader.binding() {
        catalog.publish_binding(binding).unwrap();
    }
}

/// The `.bm25` file of segment `part` in the catalog a node flushed at
/// `index_path`.
pub fn segment_bm25(index_path: &Path, part: usize) -> std::path::PathBuf {
    let root = segments_root(index_path);
    let manifest = SegmentCatalog::read_manifest(&root)
        .unwrap()
        .unwrap_or_else(|| panic!("no published catalog at {}", root.display()));
    let segment = &manifest.segments[part];
    SegmentCatalog::segment_dir(&root, &segment.segment_id).join(&segment.bm25.file)
}

/// The store saved at `bm25_path`, sealed as the one segment of the catalog
/// at `segments_root(index_path)` and served behind `tail`, an empty heap
/// store declaring the same tables.
pub fn sealed_shard(
    index_path: &Path,
    bm25_path: &Path,
    tail: pipestream_search::postings::Bm25Store,
) -> pipestream_search::node::Bm25Shard {
    seal_bm25_file(index_path, bm25_path);
    pipestream_search::node::Bm25Shard::Segmented(
        pipestream_search::segmented::SegmentedShard::open(segments_root(index_path), tail)
            .unwrap(),
    )
}

/// The vector image of segment `part` in the catalog at `index_path`.
pub fn segment_vector(index_path: &Path, part: usize) -> std::path::PathBuf {
    let root = segments_root(index_path);
    let manifest = SegmentCatalog::read_manifest(&root)
        .unwrap()
        .unwrap_or_else(|| panic!("no published catalog at {}", root.display()));
    let segment = &manifest.segments[part];
    SegmentCatalog::segment_dir(&root, &segment.segment_id).join(&segment.vector.file)
}
