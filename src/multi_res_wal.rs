//! Validate materialized vector records before mutation or WAL publication.

use crate::multi_res::RetainedVector;
use crate::pb::AddVectorsRequest;

pub(crate) fn require_tiers(
    row: &RetainedVector,
    prefixes: &[u32],
    active: Option<u32>,
) -> Result<(), String> {
    for dim in prefixes.iter().copied().chain(active) {
        if row.output.tier(dim).is_none() {
            return Err(format!("retained vector omits configured tier {dim}"));
        }
    }
    Ok(())
}

pub(crate) fn require_coverage(
    rows: &crate::multi_res::MultiResVectors,
    slots: impl Iterator<Item = usize>,
    live: &crate::live_docs::LiveDocs,
    prefixes: &[u32],
    active: Option<u32>,
) -> Result<(), String> {
    if prefixes.is_empty() && active.is_none() {
        return Ok(());
    }
    for slot in slots {
        if live.is_deleted(slot) {
            continue;
        }
        let row = rows.retained(u32::try_from(slot).map_err(|_| "retained row exceeds u32")?)
            .ok_or_else(|| format!("persisted generation is missing retained vector row {slot}; rebuild from full-resolution input"))?;
        require_tiers(row, prefixes, active)?;
    }
    Ok(())
}

pub(crate) fn decode_batch(batch: &AddVectorsRequest) -> Result<Vec<RetainedVector>, String> {
    if batch.materialized_rows.is_empty() {
        return Ok(Vec::new());
    }
    let dim = batch.dim as usize;
    if dim == 0
        || !batch.vectors.len().is_multiple_of(dim)
        || batch.materialized_rows.len() != batch.vectors.len() / dim
    {
        return Err("materialized vector count differs from active vector batch".into());
    }
    batch
        .materialized_rows
        .iter()
        .zip(batch.vectors.chunks_exact(dim))
        .map(|(bytes, active)| {
            let row = crate::multi_res::decode(bytes).map_err(|e| e.to_string())?;
            let tier = row
                .output
                .tier(batch.dim)
                .ok_or("materialized vector omits the active tier")?;
            if tier.len() != active.len()
                || tier
                    .iter()
                    .zip(active)
                    .any(|(a, b)| a.to_bits() != b.to_bits())
            {
                return Err("materialized vector disagrees with active vector bits".into());
            }
            Ok(row)
        })
        .collect()
}

#[cfg(feature = "net")]
pub(crate) fn require_supported(version: u32, batch: &AddVectorsRequest) -> Result<(), String> {
    if !batch.materialized_rows.is_empty() && version < 1 {
        return Err("target does not support materialized vector retention".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch() -> AddVectorsRequest {
        let raw_full = (1..=16).map(|v| v as f32).collect::<Vec<_>>();
        let output = crate::turbo_bridge::MultiResVectorOutput::from_full_and_sentences(
            &raw_full,
            &[8],
            None,
            Vec::new(),
        );
        let vectors = output.tier(8).unwrap().to_vec();
        let encoded = crate::multi_res::encode(&RetainedVector { output }).unwrap();
        AddVectorsRequest {
            dim: 8,
            vectors,
            materialized_rows: vec![encoded],
        }
    }

    #[test]
    fn materialized_admission_checks_bits_cardinality_and_configured_tiers() {
        let good = batch();
        let rows = decode_batch(&good).unwrap();
        require_tiers(&rows[0], &[8, 16], Some(8)).unwrap();
        assert!(require_tiers(&rows[0], &[4, 16], Some(8)).is_err());
        let mut wrong = good.clone();
        wrong.vectors[0] = f32::from_bits(wrong.vectors[0].to_bits() + 1);
        assert!(decode_batch(&wrong).err().unwrap().contains("bits"));
        let mut wrong = good;
        wrong
            .materialized_rows
            .push(wrong.materialized_rows[0].clone());
        assert!(decode_batch(&wrong).err().unwrap().contains("count"));
    }

    #[cfg(feature = "net")]
    #[test]
    fn legacy_targets_accept_plain_rows_but_refuse_retention() {
        let retained = batch();
        assert!(require_supported(0, &retained).is_err());
        require_supported(1, &retained).unwrap();
        let mut plain = retained;
        plain.materialized_rows.clear();
        require_supported(0, &plain).unwrap();
    }

    #[test]
    fn wal_publishes_version_gate_only_after_validating_materialized_rows() {
        let path = std::env::temp_dir().join(format!("retained-wal-gate-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let index =
            crate::vector::VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, 8, 4).unwrap();
        let mut manifest =
            crate::node::wal_manifest(Some(&index), &crate::node::NodeConfig::default(), 0, (0, 0));
        manifest.format_version = 8;
        let mut writer = crate::wal::WalWriter::create(&path, manifest).unwrap();
        let mut invalid = batch();
        invalid.vectors[0] = -invalid.vectors[0];
        let op = |batch| {
            crate::pb::wal::wal_record::Op::AddVectors(crate::pb::wal::LoggedAddVectors {
                first_id: 0,
                batch: Some(batch),
                stable_routing_keys: Vec::new(),
            })
        };
        assert!(writer.append(op(invalid)).is_err());
        assert_eq!(writer.manifest().format_version, 8);
        writer.append(op(batch())).unwrap();
        writer.flush().unwrap();
        assert_eq!(
            writer.manifest().format_version,
            crate::wal::MULTI_RES_FORMAT_VERSION
        );
        let persisted = std::fs::read_to_string(crate::wal::manifest_path(&path)).unwrap();
        assert!(persisted.contains("format_version = 9"));
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }
}
