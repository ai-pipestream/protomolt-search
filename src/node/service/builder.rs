//! Fresh BM25 builder construction from the shard's declared column tables.

use crate::node::*;

fn names(fields: &[String]) -> Vec<&str> {
    fields.iter().map(String::as_str).collect()
}

/// The shard's declared column names, borrowed per family.
struct ColumnTables<'a> {
    names: Vec<&'a str>,
    facets: Vec<&'a str>,
    numerics: Vec<&'a str>,
    map_facets: Vec<&'a str>,
    map_numerics: Vec<&'a str>,
    map_integers: Vec<&'a str>,
    map_unsigned_integers: Vec<&'a str>,
    integers: Vec<&'a str>,
    unsigned_integers: Vec<&'a str>,
    geos: Vec<&'a str>,
    positions: Vec<&'a str>,
    sentences: Vec<&'a str>,
}

impl<'a> ColumnTables<'a> {
    /// Borrow every table, refusing positional or sentence fields that are
    /// not BM25 fields.
    fn of(config: &'a NodeConfig) -> Result<Self, Status> {
        let tables = Self {
            names: names(&config.bm25_fields),
            facets: names(&config.facet_fields),
            numerics: names(&config.numeric_fields),
            map_facets: names(&config.map_facet_fields),
            map_numerics: names(&config.map_numeric_fields),
            map_integers: names(&config.map_integer_fields),
            map_unsigned_integers: names(&config.map_unsigned_integer_fields),
            integers: names(&config.integer_fields),
            unsigned_integers: names(&config.unsigned_integer_fields),
            geos: names(&config.geo_fields),
            positions: names(&config.position_fields),
            sentences: names(&config.sentence_fields),
        };
        let names = &tables.names;
        for (role, fields) in [
            ("positional", &tables.positions),
            ("sentence", &tables.sentences),
        ] {
            for name in fields {
                if !names.contains(name) {
                    return Err(Status::failed_precondition(format!(
                        "{role} field {name:?} is not in this shard's BM25 field table {names:?}"
                    )));
                }
            }
        }
        Ok(tables)
    }

    fn heap_builder(&self, config: &NodeConfig) -> Bm25Shard {
        let mut store = Bm25Store::with_text_fields(&self.names)
            .with_columns(crate::postings::ColumnKind::Facet, &self.facets)
            .with_columns(crate::postings::ColumnKind::Numeric, &self.numerics)
            .with_columns(crate::postings::ColumnKind::MapFacet, &self.map_facets)
            .with_columns(crate::postings::ColumnKind::MapNumeric, &self.map_numerics)
            .with_columns(crate::postings::ColumnKind::MapInteger, &self.map_integers)
            .with_columns(
                crate::postings::ColumnKind::MapUnsignedInteger,
                &self.map_unsigned_integers,
            )
            .with_columns(crate::postings::ColumnKind::Integer, &self.integers)
            .with_columns(
                crate::postings::ColumnKind::UnsignedInteger,
                &self.unsigned_integers,
            )
            .with_columns(crate::postings::ColumnKind::Geo, &self.geos)
            .with_positions(&self.positions)
            .with_sentences(&self.sentences);
        store.set_derived(stored_derived(config));
        Bm25Shard::Building(store)
    }
}

impl NodeServiceImpl {
    /// The builder shape for a fresh ingest: a persisted shard opens its
    /// segment catalog with a searchable heap tail sealed on flush; a
    /// path-less shard builds in heap.
    pub(in crate::node) fn new_builder(&self) -> Result<Bm25Shard, Status> {
        validate_column_tables(&self.config).map_err(Status::failed_precondition)?;
        let tables = ColumnTables::of(&self.config)?;
        match self.config.index_path.as_ref() {
            Some(p) => {
                let root = segments_root(p);
                let tail = heap_store(&self.config).map_err(Status::failed_precondition)?;
                SegmentedShard::open_with(&root, tail, self.config.vector_load())
                    .map(Bm25Shard::Segmented)
                    .map_err(|e| {
                        Status::internal(format!("segment catalog {}: {e}", root.display()))
                    })
            }
            None => Ok(tables.heap_builder(&self.config)),
        }
    }
}
