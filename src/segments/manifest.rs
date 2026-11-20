//! The set manifest and the per-segment records it publishes.

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentArtifact {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

/// One column's range over a sealed segment: the least and greatest
/// stored value and how many rows carry one. A segment whose range
/// cannot intersect a request's predicate holds no row that passes it,
/// so a planner may leave the segment unopened without changing the
/// answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntColumnSummary {
    pub name: String,
    pub min: i64,
    pub max: i64,
    pub present: u64,
}

/// [`IntColumnSummary`] for an unsigned column, including values above `i64::MAX`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UintColumnSummary {
    pub name: String,
    pub min: u64,
    pub max: u64,
    pub present: u64,
}

/// [`IntColumnSummary`] for a double column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NumericColumnSummary {
    pub name: String,
    pub min: f64,
    pub max: f64,
    pub present: u64,
}

/// The value range a partitioned compaction gave this segment: every
/// row that carries `column` has a value in `lo..=hi`, and the segments
/// of one set cover disjoint ascending ranges. Absent on a bucket-layout
/// segment and on the unkeyed segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRange {
    pub column: String,
    pub lo: i64,
    pub hi: i64,
}

/// Per-segment column ranges, written at seal time from the sealed
/// BM25 image. Columns with no stored value in the segment are listed
/// with `present == 0` and a `min > max` placeholder range: the full
/// integer range inverted, and for doubles `f64::MAX` over `f64::MIN`
/// (JSON has no infinities, so those are the placeholders that read
/// back).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SegmentSummary {
    #[serde(default)]
    pub int_columns: Vec<IntColumnSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uint_columns: Vec<UintColumnSummary>,
    #[serde(default)]
    pub numeric_columns: Vec<NumericColumnSummary>,
    #[serde(default)]
    pub partition: Option<PartitionRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentMetadata {
    pub segment_id: String,
    pub generation: u64,
    pub base_label: u64,
    pub rows: u64,
    pub live_rows: u64,
    pub backend_kind: String,
    pub scoring_fingerprint: String,
    pub analysis_fingerprints: Vec<u64>,
    pub document_count: u64,
    pub total_document_length: u64,
    pub vector: SegmentArtifact,
    pub exact_vectors: SegmentArtifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multi_res: Option<SegmentArtifact>,
    pub bm25: SegmentArtifact,
    pub live_docs: SegmentArtifact,
    /// Optional per-segment column summary used as a pruning accelerator;
    /// segments with `None` remain unpruned and are always scanned.
    pub summary: Option<SegmentSummary>,
}

impl SegmentMetadata {
    pub fn end_label_exclusive(&self) -> Result<u64, String> {
        self.base_label
            .checked_add(self.rows)
            .ok_or_else(|| format!("segment {:?} label range overflows", self.segment_id))
    }
}

/// Canonical protobuf binding and its checksum, published with the segment set.
/// The payload uses the complete LoggedBinding declaration shared with the WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentBinding {
    pub protobuf: Vec<u8>,
    pub sha256: String,
}

impl SegmentBinding {
    pub fn encode(binding: &StoredBinding) -> Result<Self, String> {
        let protobuf = crate::pb::wal::LoggedBinding {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: binding.body_path.clone(),
            materialize_sha: binding.materialize_sha.clone(),
            analysis_sha: binding.analysis_sha.clone(),
            analysis_contract: binding.analysis_contract.clone(),
            vector_binding: binding.vector_binding.clone(),
            index_contract: binding.index_contract.clone(),
        }
        .encode_to_vec();
        let value = Self {
            sha256: crate::sha256::hex_digest(&protobuf),
            protobuf,
        };
        value.decode()?;
        Ok(value)
    }

    pub fn decode(&self) -> Result<StoredBinding, String> {
        if self.sha256 != crate::sha256::hex_digest(&self.protobuf) {
            return Err("segment binding checksum mismatch".into());
        }
        let value = crate::pb::wal::LoggedBinding::decode(self.protobuf.as_slice())
            .map_err(|e| format!("invalid segment binding protobuf: {e}"))?;
        if value.encode_to_vec() != self.protobuf
            || value.plan_fingerprint.is_empty()
            || value.body_path.is_empty()
        {
            return Err("segment binding must be canonical with a plan and body".into());
        }
        crate::mapped_analysis::decode_contract(
            &value.analysis_sha,
            &value.analysis_contract,
            &value.body_path,
        )?;
        crate::mapped_vector::decode(&value.vector_binding, &value.plan_fingerprint)
            .map_err(|e| e.to_string())?;
        crate::index_contract::validate_binding(
            &value.index_contract,
            &value.plan_fingerprint,
            &value.vector_binding,
        )
        .map_err(|e| e.to_string())?;
        Ok(StoredBinding {
            plan_fingerprint: value.plan_fingerprint,
            body_path: value.body_path,
            materialize_sha: value.materialize_sha,
            analysis_sha: value.analysis_sha,
            analysis_contract: value.analysis_contract,
            vector_binding: value.vector_binding,
            index_contract: value.index_contract,
        })
    }
}

/// Canonical protobuf authority, including for a generation with no rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentSourceOwner {
    pub protobuf: Vec<u8>,
    pub sha256: String,
}

impl SegmentSourceOwner {
    pub fn encode(owner: &crate::pb::storage::SourceIndexOwner) -> Result<Self, String> {
        let protobuf = owner.encode_to_vec();
        let value = Self {
            sha256: crate::sha256::hex_digest(&protobuf),
            protobuf,
        };
        value.decode()?;
        Ok(value)
    }

    pub fn decode(&self) -> Result<crate::pb::storage::SourceIndexOwner, String> {
        let owner = crate::pb::storage::SourceIndexOwner::decode(self.protobuf.as_slice())
            .map_err(|e| format!("source index owner: {e}"))?;
        if self.sha256 != crate::sha256::hex_digest(&self.protobuf)
            || owner.encode_to_vec() != self.protobuf
            || owner.format_version != 1
            || owner.history_id.len() != 16
            || owner.history_id.iter().all(|b| *b == 0)
            || owner.index_key.is_empty()
            || owner.index_key.len() > 1024
        {
            return Err("invalid source index owner format, identity or checksum".into());
        }
        Ok(owner)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentSetManifest {
    pub format: u32,
    /// Generation-level metadata remains present even when no segment has rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<SegmentBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_owner: Option<SegmentSourceOwner>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_declaration: Option<SegmentGenerationDeclaration>,
    pub epoch: u64,
    pub segments: Vec<SegmentMetadata>,
    /// The integer column a partitioned compaction ordered this set by;
    /// absent for the bucket layout. A later seal appends an unordered
    /// tail segment and leaves the key in place: the ordered segments
    /// keep their ranges, and the next partitioned compaction folds the
    /// tail in.
    #[serde(default)]
    pub partition_key: Option<String>,
}

impl Default for SegmentSetManifest {
    fn default() -> Self {
        Self {
            format: SET_FORMAT,
            binding: None,
            source_owner: None,
            generation_declaration: None,
            epoch: 0,
            segments: Vec::new(),
            partition_key: None,
        }
    }
}

impl SegmentSetManifest {
    pub fn with_binding(mut self, binding: Option<&StoredBinding>) -> Result<Self, String> {
        self.binding = binding.map(SegmentBinding::encode).transpose()?;
        Ok(self)
    }
}
