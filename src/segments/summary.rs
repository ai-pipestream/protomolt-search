//! Column ranges recorded when a segment is sealed.

use super::*;

/// The column ranges of a sealed BM25 image, over every stored row
/// (deleted rows included: a range over a superset is still sound for
/// pruning, and the live bitmap keeps changing after the seal).
pub(crate) fn summarize_columns(bm25: &Bm25Reader, rows: u32) -> SegmentSummary {
    let int_columns = (0..bm25.integer_count())
        .map(|ii| {
            let mut present = 0u64;
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for doc in 0..rows {
                if let Some(value) = bm25.integer_value(ii, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            IntColumnSummary {
                name: bm25.integer_name(ii).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    let uint_columns = (0..bm25.unsigned_integer_count())
        .map(|ii| {
            let mut present = 0u64;
            let mut min = u64::MAX;
            let mut max = 0;
            for doc in 0..rows {
                if let Some(value) = bm25.unsigned_integer_value(ii, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            UintColumnSummary {
                name: bm25.unsigned_integer_name(ii).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    let numeric_columns = (0..bm25.numeric_count())
        .map(|ni| {
            let mut present = 0u64;
            let mut min = f64::MAX;
            let mut max = f64::MIN;
            for doc in 0..rows {
                if let Some(value) = bm25.numeric_value(ni, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            if present == 0 {
                min = f64::MAX;
                max = f64::MIN;
            }
            NumericColumnSummary {
                name: bm25.numeric_name(ni).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    SegmentSummary {
        int_columns,
        uint_columns,
        numeric_columns,
        partition: None,
    }
}
