use super::compaction_build::cut_partitions;

fn ids(keys: &[i64]) -> Vec<(i64, u64)> {
    keys.iter()
        .enumerate()
        .map(|(i, key)| (*key, i as u64))
        .collect()
}

fn shape(keyed: &[(i64, u64)], unkeyed: &[u64], bound: usize) -> Vec<Vec<u64>> {
    cut_partitions(keyed, unkeyed, bound)
        .into_iter()
        .map(|p| p.ids)
        .collect()
}

#[test]
fn a_cut_prefers_key_boundaries() {
    // Three rows of key 1, three of key 2, two of key 3, bound 5: the
    // run of key 2 would overflow the first partition (six rows), so
    // it moves as a unit; keys 2 and 3 then fit together in five.
    let keyed = ids(&[1, 1, 1, 2, 2, 2, 3, 3]);
    assert_eq!(
        shape(&keyed, &[], 5),
        vec![vec![0, 1, 2], vec![3, 4, 5, 6, 7]]
    );
    // At bound 4 the pair of key-3 rows does not fit either.
    assert_eq!(
        shape(&keyed, &[], 4),
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]
    );
}

#[test]
fn a_run_longer_than_the_bound_is_split() {
    let keyed = ids(&[5, 5, 5, 5, 5, 6]);
    assert_eq!(
        shape(&keyed, &[], 2),
        vec![vec![0, 1], vec![2, 3], vec![4, 5]]
    );
}

#[test]
fn unkeyed_rows_follow_in_bounded_pieces() {
    let keyed = ids(&[1, 2]);
    assert_eq!(
        shape(&keyed, &[10, 11, 12], 2),
        vec![vec![0, 1], vec![10, 11], vec![12]]
    );
}

#[test]
fn a_zero_bound_means_one_row_per_partition() {
    let keyed = ids(&[1, 1]);
    assert_eq!(shape(&keyed, &[], 0), vec![vec![0], vec![1]]);
}
