//! The exact merge of browse pages across a relay's children.

use tonic::Status;

use crate::pb::{BrowseShardRequest, BrowseShardResponse};
use crate::sortkeys::{cmp_rows, Key};

use super::merge_util::{merge_known, SegmentCounts};

/// A per-column scalar type the children report beside a known flag:
/// the children that know the column must agree on the type, and a
/// child that does not know it reports `Unspecified`.
fn merge_column_types(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<i32>>,
    share: &[i32],
) -> Result<(), Status> {
    let Some(types) = acc else {
        *acc = Some(share.to_vec());
        return Ok(());
    };
    if types.len() != share.len() {
        return Err(Status::internal(format!(
            "relay: child {shard} answered {} {what} types while an earlier child \
             answered {}",
            share.len(),
            types.len()
        )));
    }
    let unspecified = crate::pb::ScalarValueType::Unspecified as i32;
    for (column, (held, &reported)) in types.iter_mut().zip(share).enumerate() {
        if reported == unspecified {
            continue;
        }
        if *held == unspecified {
            *held = reported;
        } else if *held != reported {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} types {what} column {column} as {reported} while \
                 an earlier child typed it {held}; the column families diverge across \
                 the children"
            )));
        }
    }
    Ok(())
}

/// One row of the merged page: its sort keys, its id, and the key row
/// re-emitted as its child sent it.
struct Row {
    keys: Vec<Key>,
    id: u64,
    row: Option<crate::pb::SortKeyRow>,
}

/// The flags, types, and counters a browse merge carries beside its rows.
#[derive(Default)]
struct BrowseMeta {
    geo_known: Option<Vec<bool>>,
    filter_known: Option<Vec<bool>>,
    sort_known: Option<Vec<bool>>,
    sort_types: Option<Vec<i32>>,
    segments: SegmentCounts,
}

impl BrowseMeta {
    fn add(&mut self, child: usize, share: &BrowseShardResponse) -> Result<(), Status> {
        merge_known("geo", child, &mut self.geo_known, &share.geo_columns_known)?;
        merge_known(
            "filter",
            child,
            &mut self.filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "sort-column",
            child,
            &mut self.sort_known,
            &share.sort_columns_known,
        )?;
        merge_column_types(
            "sort",
            child,
            &mut self.sort_types,
            &share.sort_column_types,
        )?;
        self.segments
            .add(child, share.segments_total, share.segments_skipped)
    }
}

/// The children's browse pages as one page: the rows sorted by the
/// request's keys then id (id order alone when unsorted, the order the
/// root sorts a node's page in), cut to `k`, each row's keys re-emitted
/// as its child sent them; known flags OR, the sort column types must
/// agree where known, segment counts add with a check. The receipt
/// fields are the caller's to fill.
pub fn merge_browse_pages(
    req: &BrowseShardRequest,
    shares: &[BrowseShardResponse],
) -> Result<BrowseShardResponse, Status> {
    for sort in &req.sort {
        crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
    }
    let sorted = !req.sort.is_empty();
    let mut meta = BrowseMeta::default();
    let mut rows: Vec<Row> = Vec::new();
    for (child, share) in shares.iter().enumerate() {
        crate::sortkeys::validate_browse_metadata(&req.sort, share)?;
        meta.add(child, share)?;
        if sorted {
            sorted_rows(req, child, share, &mut rows)?;
        } else {
            rows.extend(share.doc_ids.iter().map(|&id| Row {
                keys: Vec::new(),
                id,
                row: None,
            }));
        }
    }
    let descending: Vec<bool> = req.sort.iter().map(|s| s.descending).collect();
    rows.sort_by(|a, b| cmp_rows(&a.keys, a.id, &b.keys, b.id, &descending));
    rows.truncate(req.k as usize);
    Ok(BrowseShardResponse {
        sort_contract_version: crate::sortkeys::MAP_SORT_CONTRACT_VERSION,
        doc_ids: rows.iter().map(|r| r.id).collect(),
        geo_columns_known: meta.geo_known.unwrap_or_default(),
        filter_columns_known: meta.filter_known.unwrap_or_default(),
        sort_rows: if sorted {
            rows.into_iter().filter_map(|r| r.row).collect()
        } else {
            Vec::new()
        },
        sort_columns_known: meta.sort_known.unwrap_or_default(),
        segments_total: meta.segments.total,
        segments_skipped: meta.segments.skipped,
        sort_column_types: meta.sort_types.unwrap_or_default(),
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    })
}

/// One child's sorted rows, each key row as wide as the request's sort
/// and typed as the child declared its columns.
fn sorted_rows(
    req: &BrowseShardRequest,
    child: usize,
    share: &BrowseShardResponse,
    rows: &mut Vec<Row>,
) -> Result<(), Status> {
    if share.sort_rows.len() != share.doc_ids.len() {
        return Err(Status::internal(format!(
            "relay: child {child} answered a sorted browse with {} key rows for {} ids",
            share.sort_rows.len(),
            share.doc_ids.len()
        )));
    }
    for (&id, row) in share.doc_ids.iter().zip(&share.sort_rows) {
        let keys: Option<Vec<Key>> = row.keys.iter().map(Key::from_pb).collect();
        let Some(keys) = keys else {
            return Err(Status::internal(format!(
                "relay: child {child} answered a sorted browse with an empty key"
            )));
        };
        if keys.len() != req.sort.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} keys for {} sort columns",
                keys.len(),
                req.sort.len()
            )));
        }
        if row.values.len() != req.sort.len() {
            return Err(Status::failed_precondition(
                "relay: sorted row has the wrong value width",
            ));
        }
        check_row_types(&keys, row, &share.sort_column_types)?;
        rows.push(Row {
            keys,
            id,
            row: Some(row.clone()),
        });
    }
    Ok(())
}

fn check_row_types(keys: &[Key], row: &crate::pb::SortKeyRow, types: &[i32]) -> Result<(), Status> {
    for ((key, value), &raw) in keys.iter().zip(&row.values).zip(types) {
        let kind = crate::pb::ScalarValueType::try_from(raw).expect("validated metadata");
        if !crate::sortkeys::key_matches_type(key, kind)
            || crate::sortkeys::value_from_pb(value).is_none_or(|v| v.column_type() != kind)
        {
            return Err(Status::failed_precondition(
                "relay: sort row disagrees with its declared column type",
            ));
        }
    }
    Ok(())
}
