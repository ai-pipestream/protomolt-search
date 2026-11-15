//! Pre-WAL materialization of NLP findings onto a document's typed
//! columns.

use tonic::Status;

use super::{NlpColumnSpec, MICROS_PER_SECOND};
use crate::pb::{
    AddDocumentsRequest, FacetValue, GeoPointValue, IntegerValue, MapFacetEntry, MapNumericEntry,
    MapUnsignedIntegerEntry, NumericValue, UnsignedIntegerValue,
};

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
        verify_logged_materialization(&doc, spec)?;
        return Ok(doc);
    }
    let Some(spec) = spec.filter(|s| s.is_enabled()) else {
        return Ok(doc);
    };
    let Some(nlp) = nlp else {
        return Ok(doc);
    };

    materialize_money(&mut doc, nlp, spec);
    materialize_quantities(&mut doc, nlp, spec);
    materialize_date(&mut doc, nlp, spec);
    materialize_geo_spine(&mut doc, nlp, spec);
    materialize_geo_point(&mut doc, nlp, spec);
    materialize_sentiment(&mut doc, nlp, spec);
    materialize_sentiment_label(&mut doc, nlp, spec);

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

    materialize_simhash(&mut doc, nlp, spec);
    materialize_minhash(&mut doc, nlp, spec);

    doc.nlp_materialization = spec.materialization_id();
    Ok(doc)
}

/// Refuses a logged document whose materialization id is malformed or
/// differs from the configured mapping's.
fn verify_logged_materialization(
    doc: &AddDocumentsRequest,
    spec: Option<&NlpColumnSpec>,
) -> Result<(), Status> {
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
    Ok(())
}

/// The maximum amount per currency, as map-numeric entries.
fn materialize_money(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The maximum value per unit, as map-numeric entries.
fn materialize_quantities(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The document date, or the earliest temporal hit, in epoch microseconds.
fn materialize_date(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// Every containment-spine prefix, country code, and canonical place name.
fn materialize_geo_spine(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The highest-confidence finite geo hit.
fn materialize_geo_point(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The signed polarity score.
fn materialize_sentiment(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The discrete polarity label.
fn materialize_sentiment_label(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The 64-bit `SimHash` sketch.
fn materialize_simhash(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
}

/// The `MinHash` signature lanes and their packed 64-bit bands.
fn materialize_minhash(
    doc: &mut AddDocumentsRequest,
    nlp: &turbo_client::DocumentNlpProjection,
    spec: &NlpColumnSpec,
) {
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
