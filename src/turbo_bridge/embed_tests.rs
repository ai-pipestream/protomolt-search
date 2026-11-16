//! Remote batch shaping: one unit row per text, zero rows recorded rather
//! than refused, and malformed responses refused by name.

use super::unit_rows;

fn reference_unit(row: &[f32]) -> Vec<f32> {
    let norm = row
        .iter()
        .map(|&v| f64::from(v) * f64::from(v))
        .sum::<f64>()
        .sqrt();
    row.iter().map(|&v| (f64::from(v) / norm) as f32).collect()
}

#[test]
fn rows_are_scaled_once_in_f64_and_keep_input_order() {
    let flat = vec![3.0, 4.0, 1.0, 1.0, 0.1, 0.7];
    let matrix = unit_rows(flat.clone(), 3, 2).unwrap();
    assert_eq!(matrix.rows(), 3);
    assert_eq!(matrix.dim(), 2);
    assert!(matrix.empty_rows().is_empty());
    for (got, raw) in matrix.view().iter_rows().zip(flat.as_chunks::<2>().0) {
        let want = reference_unit(raw);
        assert_eq!(
            got.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            want.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn zero_and_non_finite_rows_become_recorded_zero_rows() {
    let flat = vec![0.0, 0.0, 3.0, 4.0, f32::NAN, 1.0, f32::INFINITY, 2.0];
    let matrix = unit_rows(flat, 4, 2).unwrap();
    assert_eq!(matrix.empty_rows(), &[0, 2, 3]);
    assert_eq!(&matrix.data()[..2], &[0.0, 0.0]);
    assert_eq!(&matrix.data()[2..4], &[0.6, 0.8]);
    assert_eq!(&matrix.data()[4..], &[0.0; 4]);
}

#[test]
fn malformed_responses_are_refused_by_name() {
    let empty = unit_rows(Vec::new(), 1, 4).unwrap_err();
    assert_eq!(empty.code(), tonic::Code::FailedPrecondition);
    assert_eq!(empty.message(), "turbo-kserve returned no embeddings");
    let no_dim = unit_rows(vec![1.0], 1, 0).unwrap_err();
    assert_eq!(no_dim.message(), "turbo-kserve returned no embeddings");
    let short = unit_rows(vec![1.0, 2.0, 3.0], 2, 2).unwrap_err();
    assert_eq!(short.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        short.message(),
        "turbo-kserve returned 3 values for 2 rows of dimension 2"
    );
    let long = unit_rows(vec![1.0, 2.0, 3.0, 4.0], 1, 2).unwrap_err();
    assert_eq!(
        long.message(),
        "turbo-kserve returned 4 values for 1 rows of dimension 2"
    );
}
