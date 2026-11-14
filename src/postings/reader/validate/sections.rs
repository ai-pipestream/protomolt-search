//! Early column-section structural and metadata checks for `.bm25` v6/v7 payloads.

use std::io;

use super::super::super::columns::{integer_present, FACET_ABSENT, INTEGER_ABSENT};
use super::super::super::format::u16_at;
use super::{invalid, MapView, SectionValidateCtx};

pub(super) fn validate_early_column_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    expected_start = validate_facet_sections(ctx, expected_start)?;
    expected_start = validate_numeric_sections(ctx, expected_start)?;
    expected_start = validate_map_facet_sections(ctx, expected_start)?;
    expected_start = validate_map_numeric_sections(ctx, expected_start)?;
    expected_start = validate_signed_integer_sections(ctx, expected_start)?;
    validate_geo_sections(ctx, expected_start)
}

fn validate_facet_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(n_values, dict_off, ords_off)) in cols.facets.iter().enumerate() {
        if dict_off != expected_start {
            return Err(invalid(format!(
                "facet field {i}: dict section does not start at the previous section's end"
            )));
        }
        if view.walk_dict(dict_off, n_values)? != ords_off {
            return Err(invalid(format!(
                "facet field {i}: dict section does not end at the ords section"
            )));
        }
        let group_end = ords_off + 4 * n_slots;
        if group_end != cols.after_facet(i) {
            return Err(invalid(format!(
                "facet field {i}: ords section does not end at the next section's start"
            )));
        }
        for slot in 0..n_slots {
            let ord = view.u32_at(ords_off + 4 * slot)?;
            if ord != FACET_ABSENT && ord >= n_values {
                return Err(invalid(format!(
                    "facet field {i}: ordinal out of dictionary range at slot {slot}"
                )));
            }
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}

fn validate_numeric_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(min_bits, max_bits, vals_off)) in cols.numerics.iter().enumerate() {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "numeric field {i}: vals section does not start at the previous section's end"
            )));
        }
        let group_end = vals_off + 8 * n_slots;
        if group_end != cols.after_numeric(i) {
            return Err(invalid(format!(
                "numeric field {i}: vals section does not end at the next section's start"
            )));
        }
        let (mut min, mut max) = (f64::NAN, f64::NAN);
        for slot in 0..n_slots {
            let v = f64::from_bits(view.u64_at(vals_off + 8 * slot)?);
            if v.is_infinite() {
                return Err(invalid(format!(
                    "numeric field {i}: non-finite value at slot {slot}"
                )));
            }
            if !v.is_nan() {
                if min.is_nan() || v < min {
                    min = v;
                }
                if max.is_nan() || v > max {
                    max = v;
                }
            }
        }
        if min.to_bits() != min_bits || max.to_bits() != max_bits {
            return Err(invalid(format!(
                "numeric field {i}: min/max metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}

fn validate_map_facet_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(n_keys, n_values, keys_off, values_off, offsets_off, pairs_off)) in
        cols.map_facets.iter().enumerate()
    {
        if keys_off != expected_start {
            return Err(invalid(format!(
                "map-facet column {i}: keys section does not start at the previous section's end"
            )));
        }
        if view.walk_dict(keys_off, n_keys)? != values_off {
            return Err(invalid(format!(
                "map-facet column {i}: keys section does not end at the values section"
            )));
        }
        if view.walk_dict(values_off, n_values)? != offsets_off {
            return Err(invalid(format!(
                "map-facet column {i}: values section does not end at the offsets section"
            )));
        }
        if pairs_off != offsets_off + 4 * (n_slots + 1) || view.u32_at(offsets_off)? != 0 {
            return Err(invalid(format!(
                "map-facet column {i}: offsets section malformed"
            )));
        }
        let total_pairs = u64::from(view.u32_at(offsets_off + 4 * n_slots)?);
        let group_end = pairs_off + 8 * total_pairs;
        if group_end != cols.after_map_facet(i) {
            return Err(invalid(format!(
                "map-facet column {i}: pairs section does not end at the next section's start"
            )));
        }
        validate_map_facet_slots(
            view,
            i,
            n_slots,
            (n_keys, n_values),
            (offsets_off, pairs_off, total_pairs),
        )?;
        expected_start = group_end;
    }
    Ok(expected_start)
}

fn validate_map_facet_slots(
    view: &MapView<'_>,
    col_idx: usize,
    n_slots: u64,
    counts: (u32, u32),
    layout: (u64, u64, u64),
) -> io::Result<()> {
    let (n_keys, n_values) = counts;
    let (offsets_off, pairs_off, total_pairs) = layout;
    let mut prev_end = 0u64;
    for slot in 0..n_slots {
        let start = prev_end;
        let end = u64::from(view.u32_at(offsets_off + 4 * (slot + 1))?);
        if end < start || end > total_pairs {
            return Err(invalid(format!(
                "map-facet column {col_idx}: offsets not monotone at slot {slot}"
            )));
        }
        let mut prev_key: Option<u32> = None;
        for p in start..end {
            let key_ord = view.u32_at(pairs_off + 8 * p)?;
            let val_ord = view.u32_at(pairs_off + 8 * p + 4)?;
            if key_ord >= n_keys || val_ord >= n_values {
                return Err(invalid(format!(
                    "map-facet column {col_idx}: ordinal out of range at slot {slot}"
                )));
            }
            if prev_key.is_some_and(|k| key_ord <= k) {
                return Err(invalid(format!(
                    "map-facet column {col_idx}: pairs not strictly key-ordered at slot {slot}"
                )));
            }
            prev_key = Some(key_ord);
        }
        prev_end = end;
    }
    Ok(())
}

fn validate_map_numeric_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(n_keys, keys_off, offsets_off, pairs_off)) in cols.map_numerics.iter().enumerate() {
        if keys_off != expected_start {
            return Err(invalid(format!(
                "map-numeric column {i}: keys section does not start at the previous \
                 section's end"
            )));
        }
        let mut key_mm: Vec<(u64, u64)> = Vec::with_capacity(n_keys as usize);
        let mut cur = keys_off;
        for _ in 0..n_keys {
            let len = u64::from(u16_at(view.bytes_at(cur, 2)?));
            view.bytes_at(cur + 2, len)?;
            key_mm.push((view.u64_at(cur + 2 + len)?, view.u64_at(cur + 2 + len + 8)?));
            cur += 2 + len + 16;
        }
        if cur != offsets_off {
            return Err(invalid(format!(
                "map-numeric column {i}: keys section does not end at the offsets section"
            )));
        }
        if pairs_off != offsets_off + 4 * (n_slots + 1) || view.u32_at(offsets_off)? != 0 {
            return Err(invalid(format!(
                "map-numeric column {i}: offsets section malformed"
            )));
        }
        let total_pairs = u64::from(view.u32_at(offsets_off + 4 * n_slots)?);
        let group_end = pairs_off + 12 * total_pairs;
        if group_end != cols.after_map_numeric(i) {
            return Err(invalid(format!(
                "map-numeric column {i}: pairs section does not end at the next section's start"
            )));
        }
        let scanned_mm = scan_map_numeric_slots(
            view,
            i,
            n_slots,
            n_keys,
            (offsets_off, pairs_off, total_pairs),
        )?;
        for (k, (&(min_bits, max_bits), &(min, max))) in key_mm.iter().zip(&scanned_mm).enumerate()
        {
            if min.to_bits() != min_bits || max.to_bits() != max_bits {
                return Err(invalid(format!(
                    "map-numeric column {i}: min/max metadata disagrees with the values \
                     for key {k}"
                )));
            }
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}

fn scan_map_numeric_slots(
    view: &MapView<'_>,
    col_idx: usize,
    n_slots: u64,
    n_keys: u32,
    layout: (u64, u64, u64),
) -> io::Result<Vec<(f64, f64)>> {
    let (offsets_off, pairs_off, total_pairs) = layout;
    let mut scanned_mm: Vec<(f64, f64)> = vec![(f64::NAN, f64::NAN); n_keys as usize];
    let mut prev_end = 0u64;
    for slot in 0..n_slots {
        let start = prev_end;
        let end = u64::from(view.u32_at(offsets_off + 4 * (slot + 1))?);
        if end < start || end > total_pairs {
            return Err(invalid(format!(
                "map-numeric column {col_idx}: offsets not monotone at slot {slot}"
            )));
        }
        let mut prev_key: Option<u32> = None;
        for p in start..end {
            let key_ord = view.u32_at(pairs_off + 12 * p)?;
            let v = f64::from_bits(view.u64_at(pairs_off + 12 * p + 4)?);
            if key_ord >= n_keys {
                return Err(invalid(format!(
                    "map-numeric column {col_idx}: key ordinal out of range at slot {slot}"
                )));
            }
            if !v.is_finite() {
                return Err(invalid(format!(
                    "map-numeric column {col_idx}: non-finite value at slot {slot}"
                )));
            }
            if prev_key.is_some_and(|k| key_ord <= k) {
                return Err(invalid(format!(
                    "map-numeric column {col_idx}: pairs not strictly key-ordered at slot {slot}"
                )));
            }
            prev_key = Some(key_ord);
            let (min, max) = &mut scanned_mm[key_ord as usize];
            if min.is_nan() || v < *min {
                *min = v;
            }
            if max.is_nan() || v > *max {
                *max = v;
            }
        }
        prev_end = end;
    }
    Ok(scanned_mm)
}

fn validate_signed_integer_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(min_bits, max_bits, vals_off, has_presence)) in cols.integers.iter().enumerate() {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "integer field {i}: vals section does not start at the previous section's end"
            )));
        }
        let bitmap_off = vals_off + 8 * n_slots;
        let group_end = bitmap_off + if has_presence { n_slots.div_ceil(8) } else { 0 };
        if group_end != cols.after_integer(i) {
            return Err(invalid(format!(
                "integer field {i}: vals section does not end at the next section's start"
            )));
        }
        let bitmap = if has_presence {
            let bits = view.bytes_at(bitmap_off, n_slots.div_ceil(8))?;
            if !n_slots.is_multiple_of(8) && bits.last().is_some_and(|b| b >> (n_slots % 8) != 0) {
                return Err(invalid(format!(
                    "integer field {i}: nonzero presence padding"
                )));
            }
            Some(bits)
        } else {
            None
        };
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        for slot in 0..n_slots {
            let v = view.u64_at(vals_off + 8 * slot)? as i64;
            if !bitmap.map_or(v != INTEGER_ABSENT, |bits| {
                integer_present(bits, slot as usize)
            }) {
                if has_presence && v != 0 {
                    return Err(invalid(format!(
                        "integer field {i}: nonzero absent value at slot {slot}"
                    )));
                }
                continue;
            }
            min = min.min(v);
            max = max.max(v);
        }
        if min as u64 != min_bits || max as u64 != max_bits {
            return Err(invalid(format!(
                "integer field {i}: min/max metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}

fn validate_geo_sections(ctx: &SectionValidateCtx<'_>, mut expected_start: u64) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(min_lat_bits, max_lat_bits, min_lon_bits, max_lon_bits, vals_off)) in
        cols.geos.iter().enumerate()
    {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "geo field {i}: vals section does not start at the previous section's end"
            )));
        }
        let group_end = vals_off + 16 * n_slots;
        if group_end != cols.after_geo(i) {
            return Err(invalid(format!(
                "geo field {i}: vals section does not end at the next section's start"
            )));
        }
        let (mut min_lat, mut max_lat) = (f64::NAN, f64::NAN);
        let (mut min_lon, mut max_lon) = (f64::NAN, f64::NAN);
        for slot in 0..n_slots {
            let lat = f64::from_bits(view.u64_at(vals_off + 16 * slot)?);
            let lon = f64::from_bits(view.u64_at(vals_off + 16 * slot + 8)?);
            if lat.is_nan() != lon.is_nan() {
                return Err(invalid(format!(
                    "geo field {i}: half-NaN coordinate pair at slot {slot} (absence is BOTH \
                     halves NaN; one half is a point that lost the other)"
                )));
            }
            if lat.is_nan() {
                continue;
            }
            if !(lat.is_finite() && (-90.0..=90.0).contains(&lat))
                || !(lon.is_finite() && (-180.0..=180.0).contains(&lon))
            {
                return Err(invalid(format!(
                    "geo field {i}: coordinate ({lat}, {lon}) at slot {slot} is not a finite \
                     degree pair on the globe"
                )));
            }
            if min_lat.is_nan() || lat < min_lat {
                min_lat = lat;
            }
            if max_lat.is_nan() || lat > max_lat {
                max_lat = lat;
            }
            if min_lon.is_nan() || lon < min_lon {
                min_lon = lon;
            }
            if max_lon.is_nan() || lon > max_lon {
                max_lon = lon;
            }
        }
        if min_lat.to_bits() != min_lat_bits
            || max_lat.to_bits() != max_lat_bits
            || min_lon.to_bits() != min_lon_bits
            || max_lon.to_bits() != max_lon_bits
        {
            return Err(invalid(format!(
                "geo field {i}: bounding-box metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}
