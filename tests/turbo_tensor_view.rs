use pipestream_search::turbo_bridge::tensor::{MatrixView, OwnedMatrix, TensorError};

#[test]
fn packed_view_exposes_rows_without_copying() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let view = MatrixView::packed(&data, 3).unwrap();
    assert_eq!((view.rows(), view.dim()), (2, 3));
    assert_eq!(view.row(0), Some(&data[..3]));
    assert_eq!(view.row(1), Some(&data[3..]));
    assert_eq!(view.row(2), None);
    let rows: Vec<_> = view.iter_rows().collect();
    assert_eq!(rows, vec![&data[..3], &data[3..]]);
    let owned = view.to_owned();
    assert_eq!(owned.data(), &data);
    assert!(owned.empty_rows().is_empty());
    assert_eq!(owned.view().row(1), view.row(1));

    let raw = vec![1.0_f32, 2.0, 3.0, 4.0, 5.0, 6.0];
    let ptr = raw.as_ptr();
    let moved = OwnedMatrix::from_flat(raw, 3).unwrap();
    assert_eq!(moved.data().as_ptr(), ptr);
    assert_eq!((moved.rows(), moved.dim()), (2, 3));
}

#[test]
fn invalid_dimensions_and_ragged_flats_are_rejected() {
    assert_eq!(MatrixView::packed(&[1.0], 0), Err(TensorError::ZeroDim));
    assert_eq!(
        MatrixView::packed(&[1.0, 2.0, 3.0], 2),
        Err(TensorError::RaggedFlat { len: 3, dim: 2 })
    );
    assert_eq!(
        MatrixView::strided(&[1.0], 1, 3, 2),
        Err(TensorError::StrideTooSmall { dim: 3, stride: 2 })
    );
    assert_eq!(
        MatrixView::strided(&[1.0, 2.0], 2, 2, 3),
        Err(TensorError::RaggedFlat { len: 2, dim: 5 })
    );
    assert_eq!(
        OwnedMatrix::from_flat(vec![1.0], 0),
        Err(TensorError::ZeroDim)
    );
    assert!(MatrixView::strided(&[1.0], 2, 2, usize::MAX).is_err());
    let zst = &[(); usize::MAX][..];
    assert_eq!(
        MatrixView::strided(zst, 2, 2, usize::MAX),
        Err(TensorError::RaggedFlat {
            len: usize::MAX,
            dim: usize::MAX,
        })
    );
    assert_eq!(
        MatrixView::strided(zst, usize::MAX, 1, 2),
        Err(TensorError::RaggedFlat {
            len: usize::MAX,
            dim: usize::MAX,
        })
    );
    let integers = OwnedMatrix::from_flat(vec![1_u16, 2, 3, 4], 2).unwrap();
    assert_eq!(integers.view().row(1), Some(&[3, 4][..]));
}

#[test]
fn empty_strided_matrix_is_valid_and_status_uses_the_same_message() {
    let view = MatrixView::<f32>::strided(&[], 0, 4, 8).unwrap();
    assert_eq!((view.rows(), view.dim()), (0, 4));
    assert_eq!(view.row(0), None);
    let error = TensorError::StrideTooSmall { dim: 4, stride: 2 };
    let message = error.to_string();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), message);
}

#[test]
fn matryoshka_prefix_is_a_strided_zero_copy_view() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let full = MatrixView::packed(&data, 3).unwrap();
    let prefix = full.prefix(2).unwrap();
    assert_eq!(prefix.row(0), Some(&data[..2]));
    assert_eq!(prefix.row(1), Some(&data[3..5]));
    assert_eq!(prefix.to_owned().data(), &[1.0, 2.0, 4.0, 5.0]);
    assert_eq!(
        full.prefix(4),
        Err(TensorError::StrideTooSmall { dim: 4, stride: 3 })
    );
    assert_eq!(full.prefix(0), Err(TensorError::ZeroDim));
}

#[test]
fn prefix_of_an_already_strided_matrix_preserves_its_padding() {
    let data = [1.0, 2.0, 99.0, 3.0, 4.0, 99.0];
    let view = MatrixView::strided(&data, 2, 2, 3).unwrap();
    let prefix = view.prefix(1).unwrap();
    assert_eq!(prefix.row(0), Some(&data[..1]));
    assert_eq!(prefix.row(1), Some(&data[3..4]));
    assert_eq!(prefix.to_owned().data(), &[1.0, 3.0]);
}

#[test]
fn l2_normalization_records_zero_rows_and_is_idempotent() {
    let mut matrix = OwnedMatrix::from_flat(vec![3.0, 4.0, 0.0, 0.0, 1.0, 0.0], 2).unwrap();
    matrix.l2_normalize_rows();
    assert_eq!(matrix.empty_rows(), &[1]);
    let rows: Vec<_> = matrix.view().iter_rows().collect();
    assert!((rows[0][0] - 0.6).abs() < 1e-6);
    assert!((rows[0][1] - 0.8).abs() < 1e-6);
    assert_eq!(rows[1], &[0.0, 0.0]);
    assert_eq!(rows[2], &[1.0, 0.0]);
    matrix.l2_normalize_rows();
    assert_eq!(matrix.empty_rows(), &[1]);
    for row in matrix.view().iter_rows().take(2).skip(1) {
        assert!(row.iter().map(|v| v * v).sum::<f32>() <= 1.0);
    }
}

#[test]
fn empty_rows_must_be_ascending_in_range_and_zero() {
    let flat = || vec![0.0, 0.0, 1.0, 0.0, 0.0, 0.0];
    let matrix = OwnedMatrix::from_flat(flat(), 2).unwrap();
    let marked = matrix.with_empty_rows(vec![0, 2]).unwrap();
    assert_eq!(marked.empty_rows(), &[0, 2]);
    let refuse = |rows: Vec<u32>, row: u32| {
        let error = OwnedMatrix::from_flat(flat(), 2)
            .unwrap()
            .with_empty_rows(rows)
            .unwrap_err();
        assert_eq!(error, TensorError::EmptyRow { row, rows: 3 });
    };
    refuse(vec![3], 3);
    refuse(vec![2, 0], 0);
    refuse(vec![0, 0], 0);
    refuse(vec![1], 1);
    assert_eq!(
        TensorError::EmptyRow { row: 1, rows: 3 }.to_string(),
        "empty row 1 is out of order, outside 3 rows, or not zero"
    );
}
