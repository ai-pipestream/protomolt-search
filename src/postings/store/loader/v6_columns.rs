//! v7 typed column-section deserialization into [`ColumnSet`].

use std::collections::HashMap;
use std::io;

use super::super::super::columns::{
    integer_present, ColumnSet, FacetStore, GeoStore, IntStore, MapFacetStore, MapNumericStore,
    NumericStore, UintStore, FACET_ABSENT, INTEGER_ABSENT,
};
use super::super::super::format::COLUMN_KIND_MAP_I64;
use super::v6::V6ColumnMetas;
use super::{invalid, PayloadView};

type LoadedMapColumns = (
    Vec<MapFacetStore>,
    Vec<MapNumericStore>,
    Vec<crate::integer_map::Store<i64>>,
    Vec<crate::integer_map::Store<u64>>,
);

pub(super) fn decode_v6_column_set(
    metas: V6ColumnMetas,
    view: &PayloadView<'_>,
    n_slots: usize,
) -> io::Result<ColumnSet> {
    let (facets, numerics) = read_facets_and_numerics(view, n_slots, metas.facets, metas.numerics)?;
    let (map_facets, map_numerics, map_integers, map_unsigned_integers) = read_map_columns(
        view,
        n_slots,
        metas.map_facets,
        metas.map_numerics,
        metas.integer_maps,
    )?;
    let (integers, unsigned_integers, geos) = read_integers_and_geos(
        view,
        n_slots,
        metas.integers,
        metas.unsigned_integers,
        metas.geos,
    )?;
    Ok(ColumnSet {
        facets,
        numerics,
        map_facets,
        map_numerics,
        map_integers,
        map_unsigned_integers,
        integers,
        unsigned_integers,
        geos,
        binding: metas.binding,
        derived: metas.derived,
    })
}

fn read_dict(
    view: &PayloadView<'_>,
    off: u64,
    n: u32,
    err_msg: &str,
) -> io::Result<(Vec<String>, HashMap<String, u32>)> {
    let mut dict = Vec::with_capacity(n as usize);
    let mut index = HashMap::with_capacity(n as usize);
    let mut cur = off;
    for ord in 0..n {
        let len = u64::from(view.u16_at(cur)?);
        let entry =
            String::from_utf8(view.at(cur + 2, len)?.to_vec()).map_err(|_| invalid(err_msg))?;
        index.insert(entry.clone(), ord);
        dict.push(entry);
        cur += 2 + len;
    }
    Ok((dict, index))
}

fn read_facets_and_numerics(
    view: &PayloadView<'_>,
    n_slots: usize,
    facet_metas: Vec<(String, u32, u64, u64)>,
    numeric_metas: Vec<(String, u64)>,
) -> io::Result<(Vec<FacetStore>, Vec<NumericStore>)> {
    let mut facets = Vec::with_capacity(facet_metas.len());
    for (name, n_values, dict_off, ords_off) in facet_metas {
        let (dict, index) = read_dict(view, dict_off, n_values, "invalid utf-8 in facet value")?;
        let mut ords = Vec::with_capacity(n_slots);
        for slot in 0..n_slots as u64 {
            let ord = view.u32_at(ords_off + 4 * slot)?;
            if ord != FACET_ABSENT && ord >= n_values {
                return Err(invalid("facet ordinal out of dictionary range"));
            }
            ords.push(ord);
        }
        facets.push(FacetStore {
            name,
            dict,
            index,
            ords,
        });
    }
    let mut numerics = Vec::with_capacity(numeric_metas.len());
    for (name, vals_off) in numeric_metas {
        let mut vals = Vec::with_capacity(n_slots);
        for slot in 0..n_slots as u64 {
            vals.push(f64::from_bits(view.u64_at(vals_off + 8 * slot)?));
        }
        numerics.push(NumericStore { name, vals });
    }
    Ok((facets, numerics))
}

fn read_map_columns(
    view: &PayloadView<'_>,
    n_slots: usize,
    map_facet_metas: Vec<(String, u32, u32, u64, u64, u64, u64)>,
    map_numeric_metas: Vec<(String, u32, u64, u64, u64)>,
    integer_map_metas: Vec<(u8, String, u64, u64)>,
) -> io::Result<LoadedMapColumns> {
    let pair_range = |offsets_off: u64, slot: usize| -> io::Result<(u64, u64)> {
        Ok((
            u64::from(view.u32_at(offsets_off + 4 * slot as u64)?),
            u64::from(view.u32_at(offsets_off + 4 * (slot as u64 + 1))?),
        ))
    };
    let mut map_facets = Vec::with_capacity(map_facet_metas.len());
    for (name, n_keys, n_values, keys_off, values_off, offsets_off, pairs_off) in map_facet_metas {
        let (keys, key_index) =
            read_dict(view, keys_off, n_keys, "invalid utf-8 in map dictionary")?;
        let (values, value_index) = read_dict(
            view,
            values_off,
            n_values,
            "invalid utf-8 in map dictionary",
        )?;
        let mut pairs = Vec::with_capacity(n_slots);
        for slot in 0..n_slots {
            let (start, end) = pair_range(offsets_off, slot)?;
            let mut list = Vec::with_capacity((end - start) as usize);
            for p in start..end {
                list.push((
                    view.u32_at(pairs_off + 8 * p)?,
                    view.u32_at(pairs_off + 8 * p + 4)?,
                ));
            }
            pairs.push(list);
        }
        map_facets.push(MapFacetStore {
            name,
            keys,
            key_index,
            values,
            value_index,
            pairs,
        });
    }
    let map_numerics = read_map_numeric_columns(view, n_slots, map_numeric_metas)?;
    let mut map_integers = Vec::new();
    let mut map_unsigned_integers = Vec::new();
    for (kind, name, off, len) in integer_map_metas {
        if kind == COLUMN_KIND_MAP_I64 {
            map_integers.push(crate::integer_map::Store::load(
                &name,
                view.at(off, len)?,
                n_slots as u32,
            )?);
        } else {
            map_unsigned_integers.push(crate::integer_map::Store::load(
                &name,
                view.at(off, len)?,
                n_slots as u32,
            )?);
        }
    }
    Ok((
        map_facets,
        map_numerics,
        map_integers,
        map_unsigned_integers,
    ))
}

fn read_map_numeric_columns(
    view: &PayloadView<'_>,
    n_slots: usize,
    metas: Vec<(String, u32, u64, u64, u64)>,
) -> io::Result<Vec<MapNumericStore>> {
    let mut map_numerics = Vec::with_capacity(metas.len());
    for (name, n_keys, keys_off, offsets_off, pairs_off) in metas {
        let mut keys = Vec::with_capacity(n_keys as usize);
        let mut key_index = HashMap::with_capacity(n_keys as usize);
        let mut cur = keys_off;
        for ord in 0..n_keys {
            let len = u64::from(view.u16_at(cur)?);
            let entry = String::from_utf8(view.at(cur + 2, len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in map dictionary"))?;
            key_index.insert(entry.clone(), ord);
            keys.push(entry);
            cur += 2 + len + 16;
        }
        let mut pairs = Vec::with_capacity(n_slots);
        for slot in 0..n_slots {
            let start = u64::from(view.u32_at(offsets_off + 4 * slot as u64)?);
            let end = u64::from(view.u32_at(offsets_off + 4 * (slot as u64 + 1))?);
            let mut list = Vec::with_capacity((end - start) as usize);
            for p in start..end {
                list.push((
                    view.u32_at(pairs_off + 12 * p)?,
                    f64::from_bits(view.u64_at(pairs_off + 12 * p + 4)?),
                ));
            }
            pairs.push(list);
        }
        map_numerics.push(MapNumericStore {
            name,
            keys,
            key_index,
            pairs,
        });
    }
    Ok(map_numerics)
}

fn read_integers_and_geos(
    view: &PayloadView<'_>,
    n_slots: usize,
    integer_metas: Vec<(String, u64, bool)>,
    unsigned_metas: Vec<(String, u64)>,
    geo_metas: Vec<(String, u64)>,
) -> io::Result<(Vec<IntStore>, Vec<UintStore>, Vec<GeoStore>)> {
    let mut integers = Vec::with_capacity(integer_metas.len());
    for (name, vals_off, has_presence) in integer_metas {
        let mut column = IntStore::new(&name);
        let bitmap = if has_presence {
            Some(view.at(vals_off + 8 * n_slots as u64, (n_slots as u64).div_ceil(8))?)
        } else {
            None
        };
        for slot in 0..n_slots {
            let value = view.u64_at(vals_off + 8 * slot as u64)? as i64;
            if bitmap.map_or(value != INTEGER_ABSENT, |bits| integer_present(bits, slot)) {
                column.set(slot as u32, value);
            }
        }
        integers.push(column);
    }
    let mut unsigned_integers = Vec::with_capacity(unsigned_metas.len());
    for (name, vals_off) in unsigned_metas {
        let mut column = UintStore::new(&name);
        let bitmap = view.at(vals_off + 8 * n_slots as u64, (n_slots as u64).div_ceil(8))?;
        for slot in 0..n_slots {
            if integer_present(bitmap, slot) {
                column.set(slot as u32, view.u64_at(vals_off + 8 * slot as u64)?);
            }
        }
        unsigned_integers.push(column);
    }
    let mut geos = Vec::with_capacity(geo_metas.len());
    for (name, vals_off) in geo_metas {
        let mut vals = Vec::with_capacity(n_slots);
        for slot in 0..n_slots as u64 {
            vals.push((
                f64::from_bits(view.u64_at(vals_off + 16 * slot)?),
                f64::from_bits(view.u64_at(vals_off + 16 * slot + 8)?),
            ));
        }
        geos.push(GeoStore { name, vals });
    }
    Ok((integers, unsigned_integers, geos))
}
