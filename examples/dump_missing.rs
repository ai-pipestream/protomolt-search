//! List every live row of a sealed catalog that lacks an integer
//! column, with the stored text, lineage, identity, and original
//! source a reviewer needs to find the row in the source corpus.
//! Read-only: the catalog, its segments, and the live-row overlay are
//! opened, never written.
//!
//! Usage:
//!   dump_missing --child=<index path or its .segments root> --column=decided

use std::path::PathBuf;

use pipestream_search::postings::Bm25Index;

fn run() -> Result<u64, String> {
    let mut child: Option<PathBuf> = None;
    let mut column: Option<String> = None;
    for arg in std::env::args().skip(1) {
        let (name, value) = arg
            .strip_prefix("--")
            .and_then(|rest| rest.split_once('='))
            .ok_or_else(|| format!("unrecognized argument {arg:?}; every flag is --name=value"))?;
        match name {
            "child" => child = Some(PathBuf::from(value)),
            "column" => column = Some(value.to_string()),
            other => return Err(format!("unrecognized flag --{other}")),
        }
    }
    let child = child.ok_or_else(|| "--child names the child index path".to_string())?;
    let column = column.ok_or_else(|| "--column names the integer column".to_string())?;
    let is_root = child
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".segments"));
    let (root, index_path) = if is_root {
        let index_path = child.with_file_name(
            child
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".segments"))
                .unwrap_or_default(),
        );
        (child, index_path)
    } else {
        (pipestream_search::node::segments_root(&child), child)
    };
    if !root.exists() {
        return Err(format!("{}: no segment catalog", root.display()));
    }
    let set = pipestream_search::segments::OpenedSegmentSet::open(&root)?;
    let overlay_path = pipestream_search::node::live_docs_sidecar_path(&index_path);
    let overlay = if overlay_path.exists() {
        Some(
            pipestream_search::live_docs::LiveDocs::open(&overlay_path)
                .map_err(|e| format!("open {}: {e}", overlay_path.display()))?,
        )
    } else {
        None
    };
    let mut rows = 0u64;
    let mut missing = 0u64;
    for i in 0..set.len() {
        let meta = set.metadata(i);
        let bm25 = set.bm25(i);
        let live = set.live_docs(i);
        let Some(ii) = (0..bm25.integer_count()).find(|&ii| bm25.integer_name(ii) == column) else {
            return Err(format!(
                "segment {} declares no integer column {column:?}",
                meta.segment_id
            ));
        };
        let year = (0..bm25.integer_count()).find(|&ii| bm25.integer_name(ii) == "year");
        for row in 0..bm25.next_doc_id() {
            let label = meta.base_label + u64::from(row);
            if live.is_deleted(row as usize)
                || overlay
                    .as_ref()
                    .is_some_and(|overlay| overlay.is_deleted(label as usize))
            {
                continue;
            }
            rows += 1;
            if bm25.integer_value(ii, row).is_some() {
                continue;
            }
            missing += 1;
            println!("== id {label} (segment {}, row {row})", meta.segment_id);
            let text = Bm25Index::text(bm25, row).unwrap_or_default();
            let snippet: String = text.chars().take(240).collect();
            println!("text: {snippet:?}");
            if let Some(lineage) = Bm25Index::lineage(bm25, row) {
                println!(
                    "lineage: parent_id={} group_id={} span={}..{}",
                    lineage.parent_id, lineage.group_id, lineage.span_start, lineage.span_end
                );
            } else {
                println!("lineage: none");
            }
            match bm25.document_identity(row) {
                Some(identity) => println!(
                    "identity: key={:?} version={} chunk_ordinal={:?}",
                    String::from_utf8_lossy(&identity.document_key),
                    identity.version,
                    identity.chunk_ordinal
                ),
                None => println!("identity: none"),
            }
            match bm25.protobuf_source(row) {
                Ok(Some((source, ordinal))) => println!(
                    "original source: {} ({} payload bytes), chunk ordinal {ordinal:?}",
                    source.message_type,
                    source.payload.len()
                ),
                Ok(None) => println!("original source: none"),
                Err(e) => println!("original source: unreadable: {e}"),
            }
            if let Some(year) = year {
                println!("year: {:?}", bm25.integer_value(year, row));
            }
            let facets: Vec<String> = (0..bm25.facet_count())
                .filter_map(|fi| {
                    bm25.facet_ord(fi, row)
                        .map(|ord| format!("{}={:?}", bm25.facet_name(fi), bm25.facet_value(fi, ord)))
                })
                .collect();
            println!("facets: {}", facets.join(", "));
        }
    }
    println!("{rows} live rows, {missing} without {column}");
    Ok(missing)
}

fn main() {
    match run() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    }
}
