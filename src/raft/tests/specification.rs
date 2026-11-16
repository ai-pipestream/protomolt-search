use super::*;

/// The specification's document versions and its version map stay equal
/// to the code's numbers: a bumped format or protocol fails here until
/// the documents move with it.
#[test]
fn specification_versions_match_the_code() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Each document's own version line, as written under its title; the
    // map's version column is checked against it, not against a literal.
    let mut versions = BTreeMap::new();
    for doc in [
        "docs/raft-admission.md",
        "docs/raft-hosting.md",
        "docs/document-writes.md",
        "docs/raft-error-registry.md",
    ] {
        let text = std::fs::read_to_string(root.join(doc)).unwrap();
        let version = text
            .lines()
            .find_map(|line| line.trim().strip_prefix("Specification version: "))
            .unwrap_or_else(|| panic!("{doc} carries a specification version line"))
            .trim()
            .to_string();
        assert!(
            !version.is_empty() && version.chars().all(|c| c.is_ascii_digit()),
            "{doc}: specification version {version:?} is a number"
        );
        versions.insert(doc, version);
    }
    let spec = std::fs::read_to_string(root.join("docs/raft-specification.md")).unwrap();
    let protocol = crate::raft::transport::PROTOCOL_VERSION;
    let format = crate::document_catalog::ACTIVE_MANAGED_FORMAT;
    let mut rows = 0;
    for line in spec.lines() {
        let line = line.trim();
        if !line.starts_with("| `docs/") {
            continue;
        }
        let cells: Vec<&str> = line
            .trim_start_matches('|')
            .trim_end_matches('|')
            .split('|')
            .map(str::trim)
            .collect();
        assert_eq!(cells.len(), 3, "version-map row: {line}");
        let doc = cells[0].trim_matches('`');
        let version = versions
            .get(doc)
            .unwrap_or_else(|| panic!("version-map row names an unlisted document: {line}"));
        assert_eq!(cells[1], version, "document version: {line}");
        if cells[2].contains("transport protocol version") {
            assert!(
                cells[2].contains(&format!("transport protocol version {protocol}")),
                "protocol number: {line}"
            );
        }
        if cells[2].contains("catalog format") {
            assert!(
                cells[2].contains(&format!("catalog format {format}")),
                "catalog format: {line}"
            );
        }
        rows += 1;
    }
    assert_eq!(rows, 4, "one version-map row per behavioral document");
}
