//! Column-ordered browse: walk the FULL admitted set with a k-bounded heap.
//!
//! Exhaustive by construction, so the exactness certificate is trivial;
//! per-shard exact top-k by key means the coordinator's merged union contains
//! the global top-k (local rank <= global rank).

use super::browse::{BrowseEcho, BrowsePage, BrowseWalk};
use crate::node::*;
use crate::pb::ScalarValueType;
use crate::sortkeys::{cmp_candidate, Key, KeyRef, Value};

#[derive(Clone, Copy)]
enum Column {
    Numeric(usize),
    Integer(usize),
    UnsignedInteger(usize),
    Facet(usize),
    MapNumeric(usize, Option<u32>),
    MapInteger(usize, Option<u32>),
    MapUnsignedInteger(usize, Option<u32>),
    MapFacet(usize, Option<u32>),
    Parent,
    Group,
}

impl Column {
    fn value_type(column: Option<Self>) -> ScalarValueType {
        match column {
            Some(Column::Integer(_) | Column::MapInteger(..)) => ScalarValueType::Integer,
            Some(Column::Numeric(_) | Column::MapNumeric(..)) => ScalarValueType::Number,
            Some(
                Column::UnsignedInteger(_)
                | Column::MapUnsignedInteger(..)
                | Column::Parent
                | Column::Group,
            ) => ScalarValueType::UnsignedInteger,
            Some(Column::Facet(_) | Column::MapFacet(..)) => ScalarValueType::Text,
            None => ScalarValueType::Unspecified,
        }
    }

    /// A map column is known only when the shard also holds the requested key.
    fn known(column: Option<Self>) -> bool {
        match column {
            Some(
                Column::MapNumeric(_, key)
                | Column::MapInteger(_, key)
                | Column::MapUnsignedInteger(_, key)
                | Column::MapFacet(_, key),
            ) => key.is_some(),
            _ => column.is_some(),
        }
    }
}

fn resolve_map_column(
    store: &Bm25Shard,
    map: &crate::pb::MapRead,
) -> Result<Option<Column>, Status> {
    let families = [
        store.map_numeric_index(&map.column),
        store.map_integer_index(&map.column),
        store.map_unsigned_integer_index(&map.column),
        store.map_facet_index(&map.column),
    ];
    if families.iter().filter(|c| c.is_some()).count() > 1 {
        return Err(Status::invalid_argument(format!(
            "sort map column {:?} exists in more than one family on this shard",
            map.column
        )));
    }
    Ok(if let Some(ci) = families[0] {
        Some(Column::MapNumeric(
            ci,
            store.map_numeric_key_ord(ci, &map.key),
        ))
    } else if let Some(ci) = families[1] {
        Some(Column::MapInteger(
            ci,
            store.map_integer_key_ord(ci, &map.key),
        ))
    } else if let Some(ci) = families[2] {
        Some(Column::MapUnsignedInteger(
            ci,
            store.map_unsigned_integer_key_ord(ci, &map.key),
        ))
    } else {
        families[3].map(|ci| Column::MapFacet(ci, store.map_facet_key_ord(ci, &map.key)))
    })
}

fn resolve_column(
    store: &Bm25Shard,
    sort: &crate::pb::BrowseSort,
) -> Result<Option<Column>, Status> {
    let families = [
        store.numeric_index(&sort.column),
        store.integer_index(&sort.column),
        store.unsigned_integer_index(&sort.column),
        store.facet_index(&sort.column),
    ];
    if families.iter().filter(|c| c.is_some()).count() > 1 {
        return Err(Status::invalid_argument(format!(
            "sort column {:?} exists in more than one family on this shard",
            sort.column
        )));
    }
    Ok(if let Some(map) = &sort.map {
        resolve_map_column(store, map)?
    } else if let Some(ni) = store.numeric_index(&sort.column) {
        Some(Column::Numeric(ni))
    } else if let Some(ii) = store.integer_index(&sort.column) {
        Some(Column::Integer(ii))
    } else if let Some(ui) = store.unsigned_integer_index(&sort.column) {
        Some(Column::UnsignedInteger(ui))
    } else if let Some(fi) = store.facet_index(&sort.column) {
        Some(Column::Facet(fi))
    } else if sort.column == "parent_id" {
        Some(Column::Parent)
    } else if sort.column == "group_id" {
        Some(Column::Group)
    } else {
        None
    })
}

/// Reads one candidate's sort keys from the shard's columns.
struct KeyReader<'a> {
    store: &'a Bm25Shard,
    index: &'a dyn Bm25Index,
    columns: Vec<Column>,
    descending: Vec<bool>,
}

impl<'a> KeyReader<'a> {
    /// A candidate's keys, borrowed where the column lets them be, and its
    /// reported values; None when any key is absent.
    fn keys_of(&self, doc: u32) -> Option<(Vec<KeyRef<'a>>, Vec<Value>)> {
        let store = self.store;
        let mut keys = Vec::with_capacity(self.columns.len());
        let mut values = Vec::with_capacity(self.columns.len());
        for (column, desc) in self.columns.iter().zip(&self.descending) {
            let adjust = |bits: u64| if *desc { !bits } else { bits };
            match column {
                Column::Numeric(ni) => {
                    let v = store.numeric_value(*ni, doc)?;
                    keys.push(KeyRef::Bits(adjust(f64_order_bits(v))));
                    values.push(Value::Number(v));
                }
                Column::Integer(ii) => {
                    let v = store.integer_value(*ii, doc)?;
                    keys.push(KeyRef::Bits(adjust(i64_order_bits(v))));
                    values.push(Value::Integer(v));
                }
                Column::UnsignedInteger(ui) => {
                    let v = store.unsigned_integer_value(*ui, doc)?;
                    keys.push(KeyRef::UnsignedBits(adjust(v)));
                    values.push(Value::UnsignedInteger(v));
                }
                Column::Facet(fi) => {
                    let ord = store.facet_ord(*fi, doc)?;
                    let text = store.facet_value(*fi, ord);
                    keys.push(KeyRef::Text(text));
                    values.push(Value::Text(text.to_string()));
                }
                Column::MapNumeric(ci, key) => {
                    let v = store.map_numeric_value(*ci, (*key)?, doc)?;
                    keys.push(KeyRef::Bits(adjust(f64_order_bits(v))));
                    values.push(Value::Number(v));
                }
                Column::MapInteger(ci, key) => {
                    let v = store.map_integer_value(*ci, (*key)?, doc)?;
                    keys.push(KeyRef::Bits(adjust(i64_order_bits(v))));
                    values.push(Value::Integer(v));
                }
                Column::MapUnsignedInteger(ci, key) => {
                    let v = store.map_unsigned_integer_value(*ci, (*key)?, doc)?;
                    keys.push(KeyRef::UnsignedBits(adjust(v)));
                    values.push(Value::UnsignedInteger(v));
                }
                Column::MapFacet(ci, key) => {
                    let ord = store.map_facet_value_ord(*ci, (*key)?, doc)?;
                    let text = store.map_facet_value(*ci, ord);
                    keys.push(KeyRef::Text(text));
                    values.push(Value::Text(text.to_string()));
                }
                Column::Parent | Column::Group => {
                    let lineage = self.index.lineage(doc)?;
                    let v = if matches!(column, Column::Parent) {
                        lineage.parent_id
                    } else {
                        lineage.group_id
                    };
                    keys.push(KeyRef::UnsignedBits(adjust(v)));
                    values.push(Value::UnsignedInteger(v));
                }
            }
        }
        Some((keys, values))
    }
}

fn parse_boundary(
    req: &crate::pb::BrowseShardRequest,
    column_types: &[i32],
) -> Result<Option<Vec<Key>>, Status> {
    if req.first_page {
        return Ok(None);
    }
    let keys: Option<Vec<Key>> = req.after_keys.iter().map(Key::from_pb).collect();
    let keys = keys
        .ok_or_else(|| Status::invalid_argument("sorted browse boundary carries an empty key"))?;
    if keys.len() != req.sort.len() {
        return Err(Status::invalid_argument(format!(
            "sorted browse boundary has {} keys for {} sort columns",
            keys.len(),
            req.sort.len()
        )));
    }
    for (key, kind) in keys.iter().zip(column_types) {
        let kind = ScalarValueType::try_from(*kind).expect("resolved column type");
        if !crate::sortkeys::key_matches_type(key, kind) {
            return Err(Status::invalid_argument(
                "sorted browse boundary key type disagrees with the sort column; restart from the first page",
            ));
        }
    }
    Ok(Some(keys))
}

struct Row {
    keys: Vec<Key>,
    values: Vec<Value>,
    id: u64,
}

/// The k best rows, worst last (a small k: the heap's work is the comparison
/// against the worst kept row, and an insertion is a shift of at most k
/// entries).
fn top_rows(
    walk: &BrowseWalk<'_>,
    reader: &KeyReader<'_>,
    boundary: Option<&Vec<Key>>,
) -> Vec<Row> {
    let req = walk.req;
    let descending = &reader.descending;
    let k = req.k as usize;
    let mut kept: Vec<Row> = Vec::with_capacity(k + 1);
    for doc in walk.prune.admitted_slots(walk.n as u32) {
        if !walk.admits(doc) {
            continue;
        }
        // A document without a value has no honest position in
        // a column order: excluded, same stance as the filters.
        let Some((keys, values)) = reader.keys_of(doc) else {
            continue;
        };
        let id = walk.slot_offset + u64::from(doc);
        if let Some(b) = boundary {
            if cmp_candidate(&keys, id, b, req.after, descending) != std::cmp::Ordering::Greater {
                continue;
            }
        }
        if kept.len() == k {
            let worst = &kept[k - 1];
            if cmp_candidate(&keys, id, &worst.keys, worst.id, descending)
                != std::cmp::Ordering::Less
            {
                continue;
            }
        }
        let row = Row {
            keys: keys.into_iter().map(KeyRef::to_owned).collect(),
            values,
            id,
        };
        let at = kept.partition_point(|r| {
            crate::sortkeys::cmp_rows(&r.keys, r.id, &row.keys, row.id, descending)
                == std::cmp::Ordering::Less
        });
        kept.insert(at, row);
        if kept.len() > k {
            kept.pop();
        }
    }
    kept
}

pub(super) fn sorted_browse(
    walk: &BrowseWalk<'_>,
    echo: BrowseEcho<'_>,
) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
    let req = walk.req;
    let mut columns = Vec::with_capacity(req.sort.len());
    let mut known = Vec::with_capacity(req.sort.len());
    let mut column_types = Vec::with_capacity(req.sort.len());
    for sort in &req.sort {
        let column = resolve_column(walk.store, sort)?;
        column_types.push(Column::value_type(column) as i32);
        known.push(Column::known(column));
        columns.push(column);
    }
    if columns.iter().any(Option::is_none) {
        // Unknown here; the coordinator's typo rule refuses only
        // when NO shard knows a column. A shard that lacks any
        // key holds no value for it on any document, so it
        // contributes no rows.
        return echo.respond(BrowsePage {
            prune: walk.prune.stats,
            sort_columns_known: known,
            sort_column_types: column_types,
            ..Default::default()
        });
    }
    let reader = KeyReader {
        store: walk.store,
        index: walk.store.as_index(),
        columns: columns.into_iter().flatten().collect(),
        descending: req.sort.iter().map(|s| s.descending).collect(),
    };
    let boundary = parse_boundary(req, &column_types)?;
    let kept = top_rows(walk, &reader, boundary.as_ref());
    let mut doc_ids = Vec::with_capacity(kept.len());
    let mut sort_rows = Vec::with_capacity(kept.len());
    for row in kept {
        doc_ids.push(row.id);
        sort_rows.push(crate::pb::SortKeyRow {
            keys: row.keys.iter().map(Key::to_pb).collect(),
            values: row.values.iter().map(Value::to_pb).collect(),
        });
    }
    echo.respond(BrowsePage {
        prune: walk.prune.stats,
        doc_ids,
        sort_rows,
        sort_columns_known: known,
        sort_column_types: column_types,
    })
}
