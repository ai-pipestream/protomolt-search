//! The declarative mapping from one-pass NLP outputs onto typed columns.

use serde::{Deserialize, Serialize};
use tonic::Status;

/// Microseconds per second, matching CEL `timestamp(...)` values and the
/// epoch-microsecond `i64` range columns.
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
    pub(super) fn materialization_id(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("NLP column spec serializes");
        let mut hash = turbo_sha256::Sha256::new();
        hash.update(&bytes);
        let digest: String = hash.finish().0.iter().map(|b| format!("{b:02x}")).collect();
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
