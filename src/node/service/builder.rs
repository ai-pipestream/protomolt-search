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

    fn spill_builder(&self, dir: &Path, config: &NodeConfig) -> std::io::Result<Bm25Shard> {
        SpillBuilder::create_with_fields(dir, &self.names).map(|b| {
            let mut b = b
                .with_facet_fields(&self.facets)
                .with_numeric_fields(&self.numerics)
                .with_map_facet_fields(&self.map_facets)
                .with_map_numeric_fields(&self.map_numerics)
                .with_map_integer_fields(&self.map_integers)
                .with_map_unsigned_integer_fields(&self.map_unsigned_integers)
                .with_integer_fields(&self.integers)
                .with_unsigned_integer_fields(&self.unsigned_integers)
                .with_geo_fields(&self.geos)
                .with_position_fields(&self.positions)
                .with_sentence_fields(&self.sentences);
            b.set_derived(stored_derived(config));
            Bm25Shard::Spilling(b)
        })
    }

    fn heap_builder(&self, config: &NodeConfig) -> Bm25Shard {
        let mut store = Bm25Store::with_fields(&self.names)
            .with_facets(&self.facets)
            .with_numerics(&self.numerics)
            .with_map_facets(&self.map_facets)
            .with_map_numerics(&self.map_numerics)
            .with_map_integers(&self.map_integers)
            .with_map_unsigned_integers(&self.map_unsigned_integers)
            .with_integers(&self.integers)
            .with_unsigned_integers(&self.unsigned_integers)
            .with_geos(&self.geos)
            .with_positions(&self.positions)
            .with_sentences(&self.sentences);
        store.set_derived(stored_derived(config));
        Bm25Shard::Building(store)
    }
}

impl NodeServiceImpl {
    /// The builder shape for a fresh ingest: persisted shards bulk-build
    /// through the disk spiller (bounded heap, not searchable until
    /// Flush); path-less demo shards build in heap.
    pub(in crate::node) fn new_builder(
        &self,
        generation: Option<&PathBuf>,
    ) -> Result<Bm25Shard, Status> {
        validate_column_tables(&self.config).map_err(Status::failed_precondition)?;
        let tables = ColumnTables::of(&self.config)?;
        match self.config.index_path.as_ref() {
            // Segmented persisted shard: searchable heap tail sealed into the catalog on flush.
            Some(p) if self.config.layout == Layout::Segments && generation.is_none() => {
                let root = segments_root(p);
                let tail = heap_store(&self.config).map_err(Status::failed_precondition)?;
                SegmentedShard::open_with(&root, tail, self.config.vector_load())
                    .map(Bm25Shard::Segmented)
                    .map_err(|e| {
                        Status::internal(format!("segment catalog {}: {e}", root.display()))
                    })
            }
            Some(p) => {
                let dir = bm25_build_dir(&storage_paths(p, generation).2);
                tables
                    .spill_builder(&dir, &self.config)
                    .map_err(|e| Status::internal(format!("spill dir {}: {e}", dir.display())))
            }
            None => Ok(tables.heap_builder(&self.config)),
        }
    }
}
