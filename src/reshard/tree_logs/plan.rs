//! A re-placement split's inputs, checked, and how one row routes under
//! the new tree.

use std::path::PathBuf;
use std::sync::Arc;

use crate::derived::Declaration;
use crate::pb::AddDocumentsRequest;
use crate::placement::{Placement, PlacementTreeConfig};
use crate::postings::StoredBinding;
use crate::wal::WalManifest;

use super::super::child_build::manifest_derived;
use super::super::cuts::partition_key_of;
use super::super::replay::read_gens_binding;
use super::super::segment_sources::{open_segment_sources, SegmentSourceShard, SourceTables};
use super::super::split_inputs::{check_manifests, create_out_dir};
use super::super::tree_split::{
    tree_children, SpillCut, TreeChild, TreeChildLayout, TreeRowSource, TreeSplitOptions,
};
use super::super::ColumnTables;

/// Everything a re-placement split decides before it reads a row.
pub(super) struct TreePlan<'a> {
    pub(super) gens: &'a [PathBuf],
    pub(super) options: &'a TreeSplitOptions,
    pub(super) out_dir: &'a std::path::Path,
    pub(super) slot_offsets: &'a [u64],
    pub(super) placement: Placement,
    pub(super) children: Vec<TreeChild>,
    /// The first input's manifest: the children's provider state.
    pub(super) manifest: WalManifest,
    pub(super) top_generation: u64,
    pub(super) binding: Option<StoredBinding>,
    /// The placement column every row's code is rewritten in.
    pub(super) column: String,
    /// The sealed segments beside the logs, under `--from-segments`.
    pub(super) sources: Option<(Vec<SegmentSourceShard>, SourceTables)>,
    /// The sources' column tables: the segments', or the ones a log
    /// manifest recorded.
    pub(super) source_columns: Option<ColumnTables>,
    /// The declaration the children are written under.
    pub(super) declaration: Option<Arc<Declaration>>,
}

/// Where the split's outputs go and which slots each child takes.
pub(super) struct TreeTarget<'a> {
    pub(super) out_dir: &'a std::path::Path,
    pub(super) slot_offsets: &'a [u64],
}

impl<'a> TreePlan<'a> {
    pub(super) fn open(
        gens: &'a [PathBuf],
        tree: &PlacementTreeConfig,
        target: TreeTarget<'a>,
        options: &'a TreeSplitOptions,
    ) -> Result<Self, String> {
        let placement = Placement::validate(tree)?;
        let children = tree_children(&placement)?;
        super::check_options(options, children.len())?;
        if target.slot_offsets.len() != children.len() {
            return Err(format!(
                "a re-placement split needs one slot offset per child: {} offsets for {} \
                 children ({} leaves, shards summed)",
                target.slot_offsets.len(),
                children.len(),
                placement.leaves().len()
            ));
        }
        let (manifest, top_generation) = check_manifests(gens, "a re-placement split", false)?;
        let binding = read_gens_binding(gens)?;
        create_out_dir(target.out_dir)?;
        let column = placement.column().to_string();
        let mut plan = Self {
            gens,
            options,
            out_dir: target.out_dir,
            slot_offsets: target.slot_offsets,
            placement,
            children,
            manifest,
            top_generation,
            binding,
            column,
            sources: None,
            source_columns: None,
            declaration: None,
        };
        plan.open_sources()?;
        plan.declaration = plan.resolve_declaration()?;
        Ok(plan)
    }

    /// The sealed segments under `--from-segments`, and the sources'
    /// column tables either way.
    fn open_sources(&mut self) -> Result<(), String> {
        if matches!(self.options.cut, SpillCut::Column { .. })
            && matches!(self.options.layout, TreeChildLayout::SingleImage { .. })
        {
            return Err(
                "a column cut orders a child's segments; it needs the segmented layout".to_string(),
            );
        }
        if self.options.source == TreeRowSource::Segments {
            self.sources = Some(open_segment_sources(self.gens)?);
            if let Some(child) = self
                .children
                .iter()
                .find(|child| self.shards_of_leaf(child.code) > 1)
            {
                return Err(format!(
                    "leaf {:?} has {} shards, which are tiled by stable routing keys, and \
                     sealed segments carry no routing keys; give the leaf one shard or split \
                     from the logs",
                    child.leaf,
                    self.shards_of_leaf(child.code)
                ));
            }
        }
        self.source_columns = match &self.sources {
            Some((_, tables)) => Some(tables.columns.clone()),
            None => self.manifest.columns.as_ref().map(|table| ColumnTables {
                facets: table.facets.clone(),
                numerics: table.numerics.clone(),
                map_facets: table.map_facets.clone(),
                map_numerics: table.map_numerics.clone(),
                map_integers: table.map_integers.clone(),
                map_unsigned_integers: table.map_unsigned_integers.clone(),
                integers: table.integers.clone(),
                unsigned_integers: table.unsigned_integers.clone(),
                geo: table.geo.clone(),
            }),
        };
        Ok(())
    }

    /// The declaration the children are written under: the one given, or the sources' own,
    /// checked against the sources' tables and the `--derive` list.
    fn resolve_declaration(&self) -> Result<Option<Arc<Declaration>>, String> {
        let declaration: Option<Arc<Declaration>> = match &self.options.derived {
            Some(declaration) => Some(Arc::clone(declaration)),
            None => manifest_derived(&self.manifest)?
                .map(|stored| stored.declaration().map(Arc::new))
                .transpose()?,
        };
        match declaration.as_deref() {
            Some(declaration) => self.check_declaration(declaration)?,
            None if !self.options.derive.is_empty() => {
                return Err(format!(
                    "--derive={:?} needs a declaration (--derived-columns)",
                    self.options.derive
                ));
            }
            None => {}
        }
        Ok(declaration)
    }

    /// The declaration's inputs must be columns the sources declare; the
    /// sources' own derived names are outputs, not inputs.
    fn check_declaration(&self, declaration: &Declaration) -> Result<(), String> {
        let Some(columns) = self.source_columns.as_ref() else {
            return Err(
                "a split under a derived-column declaration needs the sources' column tables, \
                 which sealed segments carry and a log manifest written by a node that records \
                 them does; this log's manifest has none, so split from the segments"
                    .to_string(),
            );
        };
        let without = |names: &[String]| -> Vec<String> {
            names
                .iter()
                .filter(|name| !declaration.is_derived(name))
                .cloned()
                .collect()
        };
        let sources_tables = crate::values::NumericTypes {
            numerics: &without(&columns.numerics),
            integers: &without(&columns.integers),
            unsigned_integers: &without(&columns.unsigned_integers),
            map_numerics: &columns.map_numerics,
            map_integers: &columns.map_integers,
            map_unsigned_integers: &columns.map_unsigned_integers,
            facets: &columns.facets,
        };
        declaration
            .check_tables(&sources_tables)
            .map_err(|e| format!("derived columns: {e}"))?;
        self.check_derive_list(declaration)
    }

    /// Every `--derive` name is a column of the declaration, readable from
    /// the row source, and not the column the spill is cut on.
    fn check_derive_list(&self, declaration: &Declaration) -> Result<(), String> {
        let options = self.options;
        for name in &options.derive {
            if !declaration.is_derived(name) {
                let names: Vec<&str> = declaration
                    .columns()
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                return Err(format!(
                    "--derive={name} names no column of the declaration ({names:?})"
                ));
            }
            if options.source == TreeRowSource::Segments
                && declaration
                    .column(name)
                    .is_some_and(|column| column.reads_stable_key)
            {
                return Err(format!(
                    "--derive={name} reads the stable routing key, and sealed segments carry \
                     none; split from the logs"
                ));
            }
        }
        if let SpillCut::Column { column, .. } = &options.cut {
            if options.derive.contains(column) {
                return Err(format!(
                    "the cut column {column:?} is being derived in this run; derive it first \
                     and cut on it in a second split"
                ));
            }
        }
        Ok(())
    }

    pub(super) fn shards_of_leaf(&self, code: i64) -> usize {
        self.children
            .iter()
            .filter(|child| child.code == code)
            .count()
    }

    /// One row through the declaration: recompute what `--derive` names
    /// and restamp, or refuse a row derived under another declaration
    /// when there is none to write the children under.
    pub(super) fn rederive(
        &self,
        id: u64,
        document: AddDocumentsRequest,
        stable_key: Option<&[u8]>,
    ) -> Result<AddDocumentsRequest, String> {
        match self.declaration.as_deref() {
            Some(declaration) => declaration
                .rederive(document, &self.options.derive, stable_key)
                .map_err(|e| format!("live source id {id}: {}", e.message())),
            None if !document.derived_fingerprint.is_empty() => Err(format!(
                "live source id {id} was derived under declaration {:?}, and the children \
                 would be written under none; pass --derived-columns",
                document.derived_fingerprint
            )),
            None => Ok(document),
        }
    }

    /// Where a live row goes: the child index, the leaf's code, and
    /// whether the code changed.
    pub(super) fn route(
        &self,
        id: u64,
        document: &AddDocumentsRequest,
        stable_key: Option<&Vec<u8>>,
    ) -> Result<(usize, i64, bool), String> {
        let carried = partition_key_of(document, &self.column)?;
        let leaf = self
            .placement
            .evaluate(document)
            .map_err(|e| format!("live source id {id}: {e}"))?;
        let moved = carried != Some(leaf.code);
        let base = self
            .children
            .iter()
            .position(|child| child.code == leaf.code)
            .expect("children cover every leaf");
        let n = self.shards_of_leaf(leaf.code);
        if n == 1 {
            return Ok((base, leaf.code, moved));
        }
        let Some(key) = stable_key.filter(|key| !key.is_empty()) else {
            return Err(format!(
                "live source id {id} routes to leaf {:?}, which has {n} shards, and carries no \
                 stable key to pick one by; give the leaf one shard or rebuild the rows through \
                 routed ingest",
                leaf.name
            ));
        };
        let hash = crate::coordinator::stable_routing_hash(key);
        let index = base
            + self.children[base..base + n]
                .iter()
                .position(|child| hash >= child.hash_lo && hash <= child.hash_hi)
                .expect("the tiles cover the hash space");
        Ok((index, leaf.code, moved))
    }

    /// A child's rows take slots from its offset up to the next offset
    /// above it; the last child is unbounded.
    pub(super) fn slot_bound(&self, index: usize) -> Option<u64> {
        let offsets = self.slot_offsets;
        offsets
            .iter()
            .copied()
            .filter(|offset| *offset > offsets[index])
            .min()
    }
}
