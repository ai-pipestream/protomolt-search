//! Unified pure-Rust `turboembed` (`turbo-client` / `turbo-kserve`) integration
//! surface.
//!
//! Encapsulates:
//! - Pre-WAL materialization of one-pass NLP typed columns ([`NlpColumnSpec`],
//!   [`materialize_nlp_columns`]) and single-pass multi-resolution / sentence
//!   vectors ([`MultiResVectorOutput`]).
//! - Quantized `u8`/`u16` SPLADE 128-doc Block-Max postings and L1 rounding
//!   slack pruning ([`splade`]).
//! - One-pass `AnalyzeQuery` structured filter binding, HNSW visit-budget
//!   selection, `MappedSurfaceLexicon` stem-to-surface completion, and
//!   `QuerySpellchecker` contextual spellcheck ([`query_plan`]).
//! - Stage-2 calibrated `RerankAndHighlight` over top-`C` candidates with
//!   `DocLineage.span_start` chunk-to-parent offset composition, plus mTLS,
//!   bearer auth, priority lanes, and deadline propagation ([`rerank`]).

pub mod query_plan;
pub mod rerank;
pub mod splade;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::pb::{
    AddDocumentsRequest, FacetValue, GeoPointValue, IntegerValue, MapFacetEntry, MapNumericEntry,
    MapUnsignedIntegerEntry, NumericValue, UnsignedIntegerValue,
};

/// Microseconds per second, matching `src/cel.rs` (`timestamp(...)`) and
/// `docs/range-facets.md` epoch-microsecond `i64` columns.
pub const MICROS_PER_SECOND: i64 = 1_000_000;

/// Validates that a configured column identifier is a strict ASCII CEL
/// identifier (`^[A-Za-z_][A-Za-z0-9_]*$`) and not a CEL reserved word,
/// preventing CEL predicate injection when synthesizing structured query
/// filters.
pub fn validate_cel_identifier(slot: &str, name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument(format!(
            "NlpColumnSpec.{slot} must not be empty when validated"
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty");
    let valid_start = first.is_ascii_alphabetic() || first == '_';
    let valid_rest = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    let reserved = matches!(name, "true" | "false" | "null" | "in");
    if !valid_start || !valid_rest || reserved {
        return Err(Status::invalid_argument(format!(
            "invalid CEL column identifier in NlpColumnSpec.{slot}: {name:?} \
             (must match ^[A-Za-z_][A-Za-z0-9_]*$ and not be a CEL reserved word)"
        )));
    }
    Ok(())
}

/// Declarative mapping from one-pass [`turbo_client::DocumentNlpProjection`]
/// outputs onto native shard typed columns.
///
/// Materialized in `NodeServiceImpl::materialize_document` **before** WAL
/// logging and column indexing so WAL replay is deterministic and never
/// re-runs NLP extraction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NlpColumnSpec {
    /// `map<string, f64>` column keyed by ISO 4217 currency code (`"USD"`, `"EUR"`).
    #[serde(default)]
    pub money_column: String,
    /// `map<string, f64>` column keyed by canonical SI unit (`"kg"`, `"m"`, `"percent"`).
    #[serde(default)]
    pub quantity_column: String,
    /// `i64` epoch-microsecond column populated from `document_date` (or earliest `temporal` hit).
    #[serde(default)]
    pub date_column: String,
    /// `map<string, string>` column storing `ContainmentSpine` hierarchy prefixes, ISO country codes, and canonical place names (`"EU/DE/BE/Berlin" in geo_spine`).
    #[serde(default)]
    pub geo_spine_column: String,
    /// `geo_point` `(lat, lon)` column populated from the highest-confidence `GeoHit`.
    #[serde(default)]
    pub geo_point_column: String,
    /// `f64` polarity score column in `[-1.0, 1.0]`.
    #[serde(default)]
    pub sentiment_column: String,
    /// `facet` column storing the discrete polarity label (`"positive"`, `"neutral"`, `"negative"`).
    #[serde(default)]
    pub sentiment_label_column: String,
    /// `map<string, string>` column storing strictly **affirmed** entities (`kind -> kb_id|surface`).
    #[serde(default)]
    pub affirmed_entity_column: String,
    /// `map<string, string>` column storing **negated / uncertain** entities, isolated from affirmed hits.
    #[serde(default)]
    pub negated_entity_column: String,
    /// `u64` unsigned integer column storing the 64-bit `SimHash` near-duplicate sketch.
    #[serde(default)]
    pub simhash_column: String,
    /// `map<string, u64>` column storing `MinHash` signature lanes (`"h00"`, ...) and 64-bit LSH bands (`"b0"`, ...).
    #[serde(default)]
    pub minhash_column: String,
}

impl NlpColumnSpec {
    fn materialization_id(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("NLP column spec serializes");
        let mut hash = crate::sha256::Sha256::new();
        hash.update(&bytes);
        let digest: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
        format!("v1:{digest}")
    }

    /// Returns `true` when at least one NLP target column is configured.
    pub fn is_enabled(&self) -> bool {
        !self.money_column.is_empty()
            || !self.quantity_column.is_empty()
            || !self.date_column.is_empty()
            || !self.geo_spine_column.is_empty()
            || !self.geo_point_column.is_empty()
            || !self.sentiment_column.is_empty()
            || !self.sentiment_label_column.is_empty()
            || !self.affirmed_entity_column.is_empty()
            || !self.negated_entity_column.is_empty()
            || !self.simhash_column.is_empty()
            || !self.minhash_column.is_empty()
    }

    /// Validates every non-empty column name against strict CEL identifier syntax.
    pub fn validate(&self) -> Result<(), Status> {
        for (slot, name) in [
            ("money_column", self.money_column.as_str()),
            ("quantity_column", self.quantity_column.as_str()),
            ("date_column", self.date_column.as_str()),
            ("geo_spine_column", self.geo_spine_column.as_str()),
            ("geo_point_column", self.geo_point_column.as_str()),
            ("sentiment_column", self.sentiment_column.as_str()),
            (
                "sentiment_label_column",
                self.sentiment_label_column.as_str(),
            ),
            (
                "affirmed_entity_column",
                self.affirmed_entity_column.as_str(),
            ),
            ("negated_entity_column", self.negated_entity_column.as_str()),
            ("simhash_column", self.simhash_column.as_str()),
            ("minhash_column", self.minhash_column.as_str()),
        ] {
            if !name.is_empty() {
                validate_cel_identifier(slot, name)?;
            }
        }
        Ok(())
    }
}

/// Materializes [`turbo_client::DocumentNlpProjection`] findings onto `doc`
/// prior to WAL persistence.
///
/// Idempotent on WAL replay: if `doc.nlp_materialization` is already populated
/// (with matching mapping fingerprint) or `nlp` is `None`, `doc` is returned
/// unchanged without re-running or mutating typed columns.
pub fn materialize_nlp_columns(
    mut doc: AddDocumentsRequest,
    nlp: Option<&turbo_client::DocumentNlpProjection>,
    spec: Option<&NlpColumnSpec>,
) -> Result<AddDocumentsRequest, Status> {
    if let Some(s) = spec.filter(|s| s.is_enabled()) {
        s.validate()?;
    }
    if !doc.nlp_materialization.is_empty() {
        if !doc.nlp_materialization.starts_with("v1:")
            || doc.nlp_materialization.len() != 67
            || !doc.nlp_materialization.as_bytes()[3..]
                .iter()
                .all(u8::is_ascii_hexdigit)
            || spec
                .filter(|s| s.is_enabled())
                .is_some_and(|s| s.materialization_id() != doc.nlp_materialization)
        {
            return Err(Status::failed_precondition(
                "NLP materialization version or column mapping differs from the logged document",
            ));
        }
        return Ok(doc);
    }
    let Some(spec) = spec.filter(|s| s.is_enabled()) else {
        return Ok(doc);
    };
    let Some(nlp) = nlp else {
        return Ok(doc);
    };

    if !spec.money_column.is_empty() {
        // Map columns require unique keys per document: retain the maximum amount per currency.
        let mut by_currency: std::collections::BTreeMap<String, f64> =
            std::collections::BTreeMap::new();
        for hit in &nlp.money {
            if !hit.amount_f64.is_finite() || hit.currency.is_empty() {
                continue;
            }
            by_currency
                .entry(hit.currency.clone())
                .and_modify(|v| *v = v.max(hit.amount_f64))
                .or_insert(hit.amount_f64);
        }
        for (currency, value) in by_currency {
            if !doc
                .map_numerics
                .iter()
                .any(|m| m.field == spec.money_column && m.key == currency)
            {
                doc.map_numerics.push(MapNumericEntry {
                    field: spec.money_column.clone(),
                    key: currency,
                    value,
                });
            }
        }
    }

    if !spec.quantity_column.is_empty() {
        let mut by_unit: std::collections::BTreeMap<String, f64> =
            std::collections::BTreeMap::new();
        for hit in &nlp.quantities {
            if !hit.value_f64.is_finite() {
                continue;
            }
            let unit_key = if hit.is_percent {
                "percent".to_string()
            } else if hit.unit.is_empty() {
                continue;
            } else {
                hit.unit.clone()
            };
            by_unit
                .entry(unit_key)
                .and_modify(|v| *v = v.max(hit.value_f64))
                .or_insert(hit.value_f64);
        }
        for (unit, value) in by_unit {
            if !doc
                .map_numerics
                .iter()
                .any(|m| m.field == spec.quantity_column && m.key == unit)
            {
                doc.map_numerics.push(MapNumericEntry {
                    field: spec.quantity_column.clone(),
                    key: unit,
                    value,
                });
            }
        }
    }

    if !spec.date_column.is_empty() && !doc.integers.iter().any(|i| i.field == spec.date_column) {
        let epoch_secs = nlp
            .document_date
            .as_ref()
            .map(|d| d.epoch_seconds)
            .or_else(|| nlp.temporals.first().map(|t| t.start_epoch_seconds));
        if let Some(secs) = epoch_secs {
            doc.integers.push(IntegerValue {
                field: spec.date_column.clone(),
                value: secs.saturating_mul(MICROS_PER_SECOND),
            });
        }
    }

    if !spec.geo_spine_column.is_empty() {
        let mut paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for hit in &nlp.geo {
            if !hit.spine_path.is_empty() {
                // Emit every prefix along the containment spine ("EU", "EU/DE", "EU/DE/Berlin")
                // so region containment queries resolve in a single facet lookup.
                let mut acc = String::new();
                for part in hit.spine_path.split('/').filter(|p| !p.is_empty()) {
                    if !acc.is_empty() {
                        acc.push('/');
                    }
                    acc.push_str(part);
                    paths.insert(acc.clone());
                }
            }
            if !hit.country_code.is_empty() {
                paths.insert(hit.country_code.clone());
            }
            if !hit.canonical_name.is_empty() {
                paths.insert(hit.canonical_name.clone());
            }
        }
        for value in paths {
            if !doc
                .map_facets
                .iter()
                .any(|m| m.field == spec.geo_spine_column && m.key == value)
            {
                doc.map_facets.push(MapFacetEntry {
                    field: spec.geo_spine_column.clone(),
                    key: value.clone(),
                    value,
                });
            }
        }
    }

    if !spec.geo_point_column.is_empty()
        && !doc
            .geo_points
            .iter()
            .any(|g| g.field == spec.geo_point_column)
    {
        if let Some(best) = nlp
            .geo
            .iter()
            .filter(|g| {
                g.confidence.is_finite() && g.latitude.is_finite() && g.longitude.is_finite()
            })
            .max_by(|a, b| a.confidence.total_cmp(&b.confidence))
        {
            doc.geo_points.push(GeoPointValue {
                field: spec.geo_point_column.clone(),
                lat: best.latitude,
                lon: best.longitude,
            });
        }
    }

    if !spec.sentiment_column.is_empty()
        && !doc
            .numerics
            .iter()
            .any(|n| n.field == spec.sentiment_column)
    {
        if let Some(prob) = nlp.sentiment.probability.filter(|p| p.is_finite()) {
            let polarity = match nlp.sentiment.label.as_deref() {
                Some("negative") => -prob,
                Some("neutral") | None => 0.0,
                Some(_) => prob,
            };
            doc.numerics.push(NumericValue {
                field: spec.sentiment_column.clone(),
                value: polarity,
            });
        }
    }

    if !spec.sentiment_label_column.is_empty() {
        if let Some(label) = nlp.sentiment.label.as_deref().filter(|s| !s.is_empty()) {
            if !doc
                .facets
                .iter()
                .any(|f| f.field == spec.sentiment_label_column)
            {
                doc.facets.push(FacetValue {
                    field: spec.sentiment_label_column.clone(),
                    value: label.to_string(),
                });
            }
        }
    }

    if !spec.affirmed_entity_column.is_empty() {
        materialize_entity_map(
            &mut doc,
            &spec.affirmed_entity_column,
            &nlp.affirmed_entities,
        );
    }

    if !spec.negated_entity_column.is_empty() {
        materialize_entity_map(&mut doc, &spec.negated_entity_column, &nlp.negated_entities);
    }

    if !spec.simhash_column.is_empty()
        && nlp.sketch.word_count > 0
        && !doc
            .unsigned_integers
            .iter()
            .any(|u| u.field == spec.simhash_column)
    {
        doc.unsigned_integers.push(UnsignedIntegerValue {
            field: spec.simhash_column.clone(),
            value: nlp.sketch.simhash,
        });
    }

    if !spec.minhash_column.is_empty()
        && nlp.sketch.word_count > 0
        && !nlp.sketch.minhash.is_empty()
    {
        for (idx, &lane) in nlp.sketch.minhash.iter().enumerate() {
            let key = format!("h{idx:02}");
            if !doc
                .map_unsigned_integers
                .iter()
                .any(|m| m.field == spec.minhash_column && m.key == key)
            {
                doc.map_unsigned_integers.push(MapUnsignedIntegerEntry {
                    field: spec.minhash_column.clone(),
                    key,
                    value: u64::from(lane),
                });
            }
        }
        for (band_idx, pair) in nlp.sketch.minhash.chunks(2).enumerate() {
            let packed = match pair {
                [lo, hi] => (u64::from(*hi) << 32) | u64::from(*lo),
                [only] => u64::from(*only),
                _ => continue,
            };
            let key = format!("b{band_idx}");
            if !doc
                .map_unsigned_integers
                .iter()
                .any(|m| m.field == spec.minhash_column && m.key == key)
            {
                doc.map_unsigned_integers.push(MapUnsignedIntegerEntry {
                    field: spec.minhash_column.clone(),
                    key,
                    value: packed,
                });
            }
        }
    }

    doc.nlp_materialization = spec.materialization_id();
    Ok(doc)
}

fn materialize_entity_map(
    doc: &mut AddDocumentsRequest,
    column: &str,
    entities: &[turbo_client::EntityPolarityHit],
) {
    let mut pairs: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for ent in entities {
        let canonical = if let Some(kb) = ent.kb_id.as_deref().filter(|s| !s.is_empty()) {
            kb.to_string()
        } else if let Some(cn) = ent.canonical_name.as_deref().filter(|s| !s.is_empty()) {
            cn.to_string()
        } else {
            ent.surface.to_lowercase()
        };
        if canonical.is_empty() {
            continue;
        }
        let kind_key = if ent.kind.is_empty() {
            "entity".to_string()
        } else {
            ent.kind.to_lowercase()
        };
        pairs.entry(kind_key).or_insert_with(|| canonical.clone());
        // Also index `id:<canonical>` -> kind so multiple entities of the same
        // kind in one document are all addressable without colliding on map keys.
        pairs.insert(format!("id:{canonical}"), ent.kind.to_lowercase());
    }
    for (key, value) in pairs {
        if !doc
            .map_facets
            .iter()
            .any(|m| m.field == column && m.key == key)
        {
            doc.map_facets.push(MapFacetEntry {
                field: column.to_string(),
                key,
                value,
            });
        }
    }
}

/// Single-pass multi-resolution document embedding output.
#[derive(Clone, Debug, PartialEq)]
pub struct MultiResVectorOutput {
    /// Matryoshka L2-normalized prefix slices `(prefix_dim, values)`.
    pub prefixes: Vec<(u32, Vec<f32>)>,
    /// Full-dimension vector.
    pub full: Vec<f32>,
    /// Precomputed inverse L2 norm (`1.0 / ||raw||_2`) before normalization.
    pub inv_norm: f32,
    /// Optional packed `TurboQuant` code bytes produced in the same pass.
    pub quantized_codes: Option<Vec<u8>>,
    /// Per-sentence pooled vectors aligned with `AnalyzedField.sentences`.
    pub sentence_vectors: Vec<Vec<f32>>,
    /// Deterministic SHA-256 digest of the canonical little-endian `full` vector bytes.
    pub sha256: [u8; 32],
}

/// Active-tier flat vectors, active dimension, and retained per-slot
/// [`MultiResVectorOutput`] rows produced from a multi-row ingest batch.
pub type MatryoshkaBatchOutput = (Vec<f32>, usize, Vec<(u32, MultiResVectorOutput)>);

impl MultiResVectorOutput {
    /// Derives a multi-resolution output from a single full vector and optional
    /// sentence spans/vectors, computing normalized Matryoshka prefixes and the
    /// streaming SHA-256 row digest in one pass.
    pub fn from_full_and_sentences(
        raw_full: &[f32],
        prefix_dims: &[u32],
        quantized_codes: Option<Vec<u8>>,
        sentence_vectors: Vec<Vec<f32>>,
    ) -> Self {
        let norm_sq: f32 = raw_full.iter().map(|v| v * v).sum();
        let inv_norm = if norm_sq > 0.0 {
            1.0 / norm_sq.sqrt()
        } else {
            0.0
        };
        let full: Vec<f32> = if inv_norm > 0.0 {
            raw_full.iter().map(|v| v * inv_norm).collect()
        } else {
            raw_full.to_vec()
        };
        let mut prefixes = Vec::with_capacity(prefix_dims.len());
        for &dim in prefix_dims {
            let take = (dim as usize).min(full.len());
            let slice = &raw_full[..take];
            let p_sq: f32 = slice.iter().map(|v| v * v).sum();
            let p_inv = if p_sq > 0.0 { 1.0 / p_sq.sqrt() } else { 0.0 };
            let vec: Vec<f32> = slice.iter().map(|v| v * p_inv).collect();
            prefixes.push((take as u32, vec));
        }
        let mut le_bytes = Vec::with_capacity(full.len() * 4);
        for &v in &full {
            le_bytes.extend_from_slice(&v.to_le_bytes());
        }
        let digest_hex = crate::sha256::hex_digest(&le_bytes);
        let mut sha256 = [0u8; 32];
        for (i, slot) in sha256.iter_mut().enumerate() {
            if let Ok(b) = u8::from_str_radix(&digest_hex[i * 2..i * 2 + 2], 16) {
                *slot = b;
            }
        }
        Self {
            prefixes,
            full,
            inv_norm,
            quantized_codes,
            sentence_vectors,
            sha256,
        }
    }

    /// Returns the L2-normalized slice for `target_dim` (matching either a
    /// precomputed prefix tier or the full vector).
    pub fn tier(&self, target_dim: u32) -> Option<&[f32]> {
        if self.full.len() == target_dim as usize {
            return Some(&self.full);
        }
        self.prefixes
            .iter()
            .find(|(d, _)| *d == target_dim)
            .map(|(_, v)| v.as_slice())
    }

    /// Slices a flat row-major embedding batch of `raw_dim` into per-row
    /// [`MultiResVectorOutput`] records and extracts the active indexing tier.
    pub fn project_batch(
        vectors: &[f32],
        raw_dim: usize,
        prefix_dims: &[u32],
        active_dim: Option<u32>,
        start_slot: usize,
    ) -> Result<MatryoshkaBatchOutput, Status> {
        let mut all_prefixes = prefix_dims.to_vec();
        if let Some(ad) = active_dim {
            if ad == 0 || ad as usize > raw_dim {
                return Err(Status::invalid_argument(format!(
                    "matryoshka_active_dim {ad} exceeds input vector dim {raw_dim}"
                )));
            }
            if !all_prefixes.contains(&ad) && (ad as usize) < raw_dim {
                all_prefixes.push(ad);
            }
        }
        let effective_dim = active_dim.map_or(raw_dim, |d| d as usize);
        let row_count = vectors.len() / raw_dim;
        let mut active_flat = Vec::with_capacity(row_count * effective_dim);
        let mut multi_rows = Vec::with_capacity(row_count);
        for (i, row) in vectors.chunks_exact(raw_dim).enumerate() {
            let slot = u32::try_from(start_slot + i)
                .map_err(|_| Status::resource_exhausted("shard local slot overflow"))?;
            let out = Self::from_full_and_sentences(row, &all_prefixes, None, Vec::new());
            let tier = out.tier(effective_dim as u32).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "matryoshka active tier {effective_dim} missing from row"
                ))
            })?;
            active_flat.extend_from_slice(tier);
            multi_rows.push((slot, out));
        }
        Ok((active_flat, effective_dim, multi_rows))
    }
}

/// Converts an absolute Unix-epoch microsecond `finish_deadline_us` into a
/// remaining [`Duration`] for outgoing `grpc-timeout` propagation.
pub fn remaining_deadline_duration(finish_deadline_us: u64) -> Result<Option<Duration>, Status> {
    if finish_deadline_us == 0 {
        return Ok(None);
    }
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    if finish_deadline_us <= now_us {
        return Err(Status::deadline_exceeded(
            "request deadline expired before dispatching to turbo-kserve",
        ));
    }
    Ok(Some(Duration::from_micros(finish_deadline_us - now_us)))
}

/// Client mTLS and server CA material for `turbo-kserve` channels.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MtlsClientMaterial {
    /// PEM-encoded root CA bundle used to verify `turbo-kserve`.
    pub server_ca_pem: Vec<u8>,
    /// Optional `(client_cert_pem, client_key_pem)` pair for mutual TLS.
    pub client_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// Optional explicit SNI / SAN server domain override.
    pub domain_name: Option<String>,
}

/// Remote `turbo-kserve` security, priority lane, and deadline policy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KserveCallPolicy {
    /// Bearer credential (`authorization: Bearer <token>`).
    pub bearer_token: Option<String>,
    /// Optional PEM-encoded server CA and client mTLS identity.
    pub mtls: Option<MtlsClientMaterial>,
}

impl KserveCallPolicy {
    /// Builds a [`turbo_client::RemoteEndpointConfig`] for `endpoint` with the
    /// requested priority lane and optional per-call timeout.
    #[cfg(feature = "net")]
    pub fn endpoint_config(
        &self,
        endpoint: &str,
        priority: turbo_client::RemotePriority,
        timeout: Option<Duration>,
    ) -> turbo_client::RemoteEndpointConfig {
        let mut cfg = turbo_client::RemoteEndpointConfig::new(endpoint).with_priority(priority);
        if let Some(tok) = &self.bearer_token {
            cfg = cfg.with_bearer_token(tok.clone());
        }
        if let Some(dur) = timeout {
            cfg = cfg.with_timeout(dur);
        }
        #[cfg(feature = "tls")]
        if let Some(mtls) = &self.mtls {
            cfg.tls = Some(turbo_client::RemoteTlsConfig {
                server_ca_pem: mtls.server_ca_pem.clone(),
                client_identity_pem: mtls.client_identity_pem.clone(),
                domain_name: mtls.domain_name.clone(),
            });
        }
        cfg
    }
}
