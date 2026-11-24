//! Judging a probe embedding against the index it is meant to query.

/// Judge one query embedding from an embedding provider: `Ok` describes a
/// vector the index can search, `Err` says why it cannot. `index_dim` 0
/// accepts any dimension.
pub fn judge_query_embedding(vector: &[f32], index_dim: usize) -> Result<String, String> {
    if index_dim > 0 && vector.len() != index_dim {
        return Err(format!(
            "dim {} where the index expects {index_dim}",
            vector.len()
        ));
    }
    if vector.iter().any(|x| !x.is_finite()) {
        return Err("non-finite coordinates".to_string());
    }
    let norm = vector
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    Ok(format!("dim {}, L2 norm {norm:.4}", vector.len()))
}

#[cfg(test)]
mod tests {
    use super::judge_query_embedding;

    #[test]
    fn a_vector_of_the_index_dimension_is_described() {
        assert_eq!(
            judge_query_embedding(&[0.6, 0.8], 2),
            Ok("dim 2, L2 norm 1.0000".to_string())
        );
        assert!(judge_query_embedding(&[0.6, 0.8], 0).is_ok());
    }

    #[test]
    fn a_model_of_another_dimension_is_refused() {
        assert_eq!(
            judge_query_embedding(&[0.0; 768], 256),
            Err("dim 768 where the index expects 256".to_string())
        );
        assert!(judge_query_embedding(&[], 256).is_err());
    }

    #[test]
    fn non_finite_coordinates_are_refused() {
        for bad in [f32::NAN, f32::INFINITY] {
            assert_eq!(
                judge_query_embedding(&[1.0, bad], 2),
                Err("non-finite coordinates".to_string())
            );
        }
    }
}
