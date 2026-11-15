//! The per-generation manifest and its validation.

use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use super::layout::{manifest_path, FORMAT_VERSION};

/// The shard shape of one WAL generation (`manifest.toml`). Resharding a
/// generation requires complete provider configuration that reproduces its
/// score semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalManifest {
    /// Vector dimensionality. 0 while the shard has not committed to one.
    pub dim: u32,
    /// Provider identifier for the vector generation; empty until the shard
    /// commits to a provider.
    #[serde(default)]
    pub vector_backend: String,
    /// Media type/version for the opaque provider configuration.
    #[serde(default)]
    pub vector_config_format: String,
    /// Provider-owned construction state. The product never interprets it.
    #[serde(default)]
    pub vector_config_payload: Vec<u8>,
    /// The collection this generation belongs to;
    /// empty in a manifest written before collections, which the first
    /// open under a named node writes. A node of another collection
    /// refuses the log by name.
    #[serde(default)]
    pub collection: String,
    /// The shard's global id base for this generation.
    pub slot_offset: u64,
    /// Generation number (matches the directory name; 0 for the initial
    /// generation, +1 per snapshot rotation).
    pub generation: u64,
    /// log2 of `bucket_count`; the top `bucket_bits` of `fnv1a64(id)`
    /// select the bucket file.
    pub bucket_bits: u32,
    /// Number of bucket files (a power of two), fixed at WAL creation.
    pub bucket_count: u32,
    /// Vectors the shard already held when this generation was created —
    /// state the log does NOT contain. Nonzero after an InstallSnapshot
    /// rotation (the image's contents) or when logging was enabled on an
    /// already-populated shard. A generation with preexisting state is
    /// not full history: replaying it alone reproduces only the records
    /// logged since, so the reshard tool refuses it.
    #[serde(default)]
    pub preexisting_vectors: u64,
    /// Documents the shard already held when this generation was created;
    /// see [`Self::preexisting_vectors`].
    #[serde(default)]
    pub preexisting_documents: u64,
    /// On-disk format version; [`FORMAT_VERSION`].
    pub format_version: u32,
    /// The derived-column declaration the log's records were derived
    /// under, by fingerprint; empty for a
    /// log written without derived columns, or before them.
    #[serde(default)]
    pub derived_fingerprint: String,
    /// The declaration itself, as a `[[derived]]` table, so a replay
    /// tool can rebuild the columns the records carry and refuse a
    /// declaration that differs.
    #[serde(default)]
    pub derived: Vec<crate::derived::DerivedColumnConfig>,
    /// The complete column table of the shard the log describes, empty
    /// columns included, in table order: what a rebuilt child declares
    /// so its tables match the parent's record for record. `None` in a
    /// manifest from before the table was recorded.
    #[serde(default)]
    pub columns: Option<ColumnTable>,
}

/// The complete column table of one shard, in table order
/// (`WalManifest::columns`): the names a store declares whether or not
/// any row carries a value under them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnTable {
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub facets: Vec<String>,
    #[serde(default)]
    pub numerics: Vec<String>,
    #[serde(default)]
    pub map_facets: Vec<String>,
    #[serde(default)]
    pub map_numerics: Vec<String>,
    #[serde(default)]
    pub map_integers: Vec<String>,
    #[serde(default)]
    pub map_unsigned_integers: Vec<String>,
    #[serde(default)]
    pub integers: Vec<String>,
    #[serde(default)]
    pub unsigned_integers: Vec<String>,
    #[serde(default)]
    pub geo: Vec<String>,
}

impl ColumnTable {
    /// The table a node's configuration declares (derived names
    /// included, after their source columns).
    pub fn of_config(config: &crate::node::NodeConfig) -> Self {
        ColumnTable {
            fields: config.bm25_fields.clone(),
            facets: config.facet_fields.clone(),
            numerics: config.numeric_fields.clone(),
            map_facets: config.map_facet_fields.clone(),
            map_numerics: config.map_numeric_fields.clone(),
            map_integers: config.map_integer_fields.clone(),
            map_unsigned_integers: config.map_unsigned_integer_fields.clone(),
            integers: config.integer_fields.clone(),
            unsigned_integers: config.unsigned_integer_fields.clone(),
            geo: config.geo_fields.clone(),
        }
    }

    /// The first table that differs from `other`, by name.
    pub fn first_difference(&self, other: &ColumnTable) -> Option<String> {
        for (name, mine, theirs) in [
            ("field", &self.fields, &other.fields),
            ("facet", &self.facets, &other.facets),
            ("numeric", &self.numerics, &other.numerics),
            ("map-facet", &self.map_facets, &other.map_facets),
            ("map-numeric", &self.map_numerics, &other.map_numerics),
            ("map-integer", &self.map_integers, &other.map_integers),
            (
                "map-unsigned-integer",
                &self.map_unsigned_integers,
                &other.map_unsigned_integers,
            ),
            ("integer", &self.integers, &other.integers),
            (
                "unsigned-integer",
                &self.unsigned_integers,
                &other.unsigned_integers,
            ),
            ("geo", &self.geo, &other.geo),
        ] {
            if mine != theirs {
                return Some(format!("{name} table {mine:?} vs {theirs:?}"));
            }
        }
        None
    }
}

/// A resumed log's declaration and column table must be the node's:
/// the same derived fingerprint, and the
/// same tables in the same order. A manifest that recorded neither is
/// adopted by the caller; one that recorded either and disagrees is
/// refused naming the difference.
pub fn check_manifest_tables(held: &WalManifest, fresh: &WalManifest) -> Result<(), String> {
    if !held.derived_fingerprint.is_empty() || !held.derived.is_empty() {
        if held.derived_fingerprint != fresh.derived_fingerprint {
            return Err(format!(
                "the log was written under derived-column declaration {:?}, this node declares \
                 {:?}; a changed declaration is a rebuild through the reshard tool",
                held.derived_fingerprint, fresh.derived_fingerprint
            ));
        }
        if held.derived != fresh.derived {
            return Err(
                "the log's [[derived]] table differs from this node's although the \
                 fingerprints agree; the manifest was edited by hand"
                    .to_string(),
            );
        }
    }
    if let (Some(held), Some(fresh)) = (held.columns.as_ref(), fresh.columns.as_ref()) {
        if let Some(difference) = held.first_difference(fresh) {
            return Err(format!(
                "the log records a column table this node does not declare: {difference}; the \
                 tables of a shard and its log agree name for name and in order"
            ));
        }
    }
    Ok(())
}

impl WalManifest {
    /// The provider state the generation's vectors were built under.
    pub fn backend_config(&self) -> Result<crate::vector::VectorBackendConfig, String> {
        if self.vector_backend.is_empty() || self.vector_config_format.is_empty() {
            return Err(format!(
                "manifest has backend {:?} but no provider configuration",
                self.vector_backend
            ));
        }
        Ok(crate::vector::VectorBackendConfig {
            backend_kind: self.vector_backend.clone(),
            config_format: self.vector_config_format.clone(),
            payload: self.vector_config_payload.clone(),
        })
    }

    pub fn set_backend_config(&mut self, config: crate::vector::VectorBackendConfig) {
        self.vector_backend = config.backend_kind;
        self.vector_config_format = config.config_format;
        self.vector_config_payload = config.payload;
    }
}

/// Write the manifest atomically (tmp file + fsync + rename + parent
/// fsync). A manifest that vanishes in a crash makes the whole
/// generation invisible to replay, so the rename's directory entry
/// gets the same durability as the bytes.
///
/// The tmp name is unique per writer (pid + counter): two nodes
/// cold-starting the same first generation write their manifests
/// concurrently, and with one shared `manifest.toml.tmp` the first
/// rename consumed the path and the second `rename` failed ENOENT —
/// the fleet's `open WAL ... No such file or directory` panic.
pub fn write_manifest(gen_dir: &Path, manifest: &WalManifest) -> io::Result<()> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp = gen_dir.join(format!(
        "manifest.toml.tmp.{}.{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(
            toml::to_string(manifest)
                .map_err(io::Error::other)?
                .as_bytes(),
        )?;
        f.sync_all()?;
    }
    let dst = manifest_path(gen_dir);
    std::fs::rename(&tmp, &dst)?;
    crate::postings::fsync_parent(&dst)
}

/// Read and validate a generation's manifest.
pub fn read_manifest(gen_dir: &Path) -> io::Result<WalManifest> {
    let text = std::fs::read_to_string(manifest_path(gen_dir))?;
    let manifest: WalManifest = toml::from_str(&text)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad manifest: {e}")))?;
    check_version(&manifest)?;
    if !manifest.bucket_count.is_power_of_two()
        || manifest.bucket_count == 0
        || 1u64 << manifest.bucket_bits != u64::from(manifest.bucket_count)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "bad bucket geometry: bucket_bits={} bucket_count={}",
                manifest.bucket_bits, manifest.bucket_count
            ),
        ));
    }
    validate_derived(&manifest)?;
    Ok(manifest)
}

pub(super) fn check_version(manifest: &WalManifest) -> io::Result<()> {
    if manifest.format_version == FORMAT_VERSION {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "unsupported WAL format version {} (this build reads only {FORMAT_VERSION})",
            manifest.format_version
        ),
    ))
}

/// The manifest's derived declaration must decode; checked before any record
/// is read or appended.
pub(super) fn validate_derived(manifest: &WalManifest) -> io::Result<()> {
    crate::reshard::manifest_derived(manifest)
        .map(|_| ())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
