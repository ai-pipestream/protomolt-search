//! Exact range intervals shared by nodes, coordinators and relays.
use crate::{
    filter::{cmp_f64_i64, cmp_f64_u64, NumBound},
    pb::{filter_bound::Value, RangeBucket, RangeFacetCounts, RangeFacetField},
};
use std::cmp::Ordering;
use tonic::Status;

fn compare(a: NumBound, b: NumBound) -> Ordering {
    use NumBound::{F, I, U};
    match (a, b) {
        (I(a), I(b)) => a.cmp(&b),
        (U(a), U(b)) => a.cmp(&b),
        (I(a), U(b)) => i128::from(a).cmp(&i128::from(b)),
        (U(a), I(b)) => i128::from(a).cmp(&i128::from(b)),
        (F(a), I(b)) => cmp_f64_i64(a, b),
        (F(a), U(b)) => cmp_f64_u64(a, b),
        (I(a), F(b)) => cmp_f64_i64(b, a).reverse(),
        (U(a), F(b)) => cmp_f64_u64(b, a).reverse(),
        (F(a), F(b)) => a
            .partial_cmp(&b)
            .expect("numeric columns and edges exclude NaN"),
    }
}

fn display(value: NumBound) -> f64 {
    match value {
        NumBound::I(v) => v as f64,
        NumBound::U(v) => v as f64,
        NumBound::F(v) => v,
    }
}

impl RangeFacetField {
    /// Literal map key; presence distinguishes an empty key from a plain column.
    pub(crate) fn map_key(&self) -> Option<&str> {
        self.map
            .as_ref()
            .map(|map| map.key.as_str())
            .or_else(|| (!self.key.is_empty()).then_some(self.key.as_str()))
    }
}

pub(crate) struct Intervals<'a> {
    request: &'a RangeFacetField,
    edges: Vec<NumBound>,
    key: Option<&'a str>,
    typed_edges: &'a [crate::pb::FilterBound],
}
impl<'a> Intervals<'a> {
    pub(crate) fn new(request: &'a RangeFacetField) -> Result<Self, Status> {
        let invalid = |why| {
            Status::invalid_argument(format!(
                "range facet {:?}[{:?}]: {why}",
                request.column,
                request.map_key().unwrap_or_default()
            ))
        };
        if request.column.is_empty() {
            return Err(invalid("a request names the column it buckets"));
        }
        let key = request.map_key();
        let (raw_edges, typed_edges) = match &request.map {
            Some(map) => {
                if !request.key.is_empty()
                    || !request.edges.is_empty()
                    || !request.typed_edges.is_empty()
                {
                    return Err(invalid(
                        "map input carries its own key and edges; leave legacy fields empty",
                    ));
                }
                (&map.edges, &map.typed_edges)
            }
            None => (&request.edges, &request.typed_edges),
        };
        if !raw_edges.is_empty() && !typed_edges.is_empty() {
            return Err(invalid("supply either edges or typed_edges, never both"));
        }
        let edges = if typed_edges.is_empty() {
            raw_edges
                .iter()
                .copied()
                .map(NumBound::F)
                .collect::<Vec<_>>()
        } else {
            typed_edges
                .iter()
                .map(|edge| {
                    if edge.exclusive {
                        return Err(invalid(
                            "typed_edges must have exclusive=false; buckets are half-open",
                        ));
                    }
                    match edge.value {
                        Some(Value::Int(v)) => Ok(NumBound::I(v)),
                        Some(Value::Uint(v)) => Ok(NumBound::U(v)),
                        Some(Value::Num(v)) => Ok(NumBound::F(v)),
                        None => Err(invalid("every typed edge must set a value")),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if edges.len() < 2 {
            return Err(invalid("edges must hold at least 2 values (one bucket)"));
        }
        if edges
            .iter()
            .any(|e| matches!(e, NumBound::F(v) if !v.is_finite()))
        {
            return Err(invalid("an edge is not finite"));
        }
        if edges
            .windows(2)
            .any(|pair| compare(pair[0], pair[1]) != Ordering::Less)
        {
            return Err(invalid("edges must be strictly ascending"));
        }
        Ok(Self {
            request,
            edges,
            key,
            typed_edges,
        })
    }
    pub(crate) fn len(&self) -> usize {
        self.edges.len() - 1
    }
    pub(crate) fn bucket(&self, value: NumBound) -> Option<usize> {
        let upper = self
            .edges
            .partition_point(|e| compare(*e, value) != Ordering::Greater);
        (upper > 0 && upper < self.edges.len()).then(|| upper - 1)
    }
    pub(crate) fn response(&self, counts: Vec<u64>, known: bool) -> RangeFacetCounts {
        debug_assert_eq!(counts.len(), self.len());
        RangeFacetCounts {
            column: self.request.column.clone(),
            key: self.key.unwrap_or_default().to_owned(),
            map_key: self.key.map(str::to_owned),
            known,
            buckets: if known {
                counts
                    .into_iter()
                    .enumerate()
                    .map(|(i, count)| RangeBucket {
                        from: display(self.edges[i]),
                        to: display(self.edges[i + 1]),
                        count,
                        typed_from: self.typed_edges.get(i).cloned(),
                        typed_to: self.typed_edges.get(i + 1).cloned(),
                    })
                    .collect()
            } else {
                vec![]
            },
        }
    }
    fn validate_response(&self, response: &RangeFacetCounts) -> Result<(), Status> {
        let malformed = || {
            Status::failed_precondition(format!(
                "range facet {:?}[{:?}]: child response does not match requested intervals",
                self.request.column,
                self.key.unwrap_or_default()
            ))
        };
        if response.column != self.request.column || response.key != self.key.unwrap_or_default() {
            return Err(malformed());
        }
        let legacy_nonempty_map =
            self.request.map.is_none() && self.key.is_some_and(|key| !key.is_empty());
        if response.map_key.as_deref() != self.key
            && !(legacy_nonempty_map && response.map_key.is_none())
        {
            return Err(malformed());
        }
        if !response.known {
            return if response.buckets.is_empty() {
                Ok(())
            } else {
                Err(malformed())
            };
        }
        if response.buckets.len() != self.len() {
            return Err(malformed());
        }
        let expected = self.response(vec![0; self.len()], true);
        for (actual, expected) in response.buckets.iter().zip(expected.buckets) {
            if actual.from != expected.from
                || actual.to != expected.to
                || actual.typed_from != expected.typed_from
                || actual.typed_to != expected.typed_to
            {
                return Err(malformed());
            }
        }
        Ok(())
    }
}

/// A relay permits an unresolved column; the root requires at least one owner.
/// Both validate identities, intervals and count overflow before summing.
pub(crate) fn merge(
    requested: &[RangeFacetField],
    shares: &[Vec<RangeFacetCounts>],
    require_known: bool,
) -> Result<Vec<RangeFacetCounts>, Status> {
    let plans = requested
        .iter()
        .map(Intervals::new)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sums: Vec<_> = plans.iter().map(|p| vec![0u64; p.len()]).collect();
    let mut known = vec![false; plans.len()];
    for share in shares {
        if share.len() != plans.len() {
            return Err(Status::failed_precondition(
                "range facets: child response has wrong field count",
            ));
        }
        for (i, (plan, response)) in plans.iter().zip(share).enumerate() {
            plan.validate_response(response)?;
            known[i] |= response.known;
            for (sum, bucket) in sums[i].iter_mut().zip(&response.buckets) {
                *sum = sum.checked_add(bucket.count).ok_or_else(|| {
                    Status::out_of_range(format!(
                        "range facet {:?}: bucket count overflows u64",
                        requested[i].column
                    ))
                })?;
            }
        }
    }
    plans
        .iter()
        .zip(sums)
        .zip(known)
        .map(|((plan, counts), known)| {
            if require_known && !known {
                return Err(Status::invalid_argument(format!(
                "no shard has range-facet column {:?}[{:?}]; check --numeric-fields / --integer-fields / --unsigned-integer-fields / --map-numeric-fields",
                plan.request.column, plan.key.unwrap_or_default()
            )));
            }
            Ok(plan.response(counts, known))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::FilterBound;
    fn typed(values: Vec<Value>) -> RangeFacetField {
        RangeFacetField {
            column: "value".into(),
            typed_edges: values
                .into_iter()
                .map(|value| FilterBound {
                    value: Some(value),
                    exclusive: false,
                })
                .collect(),
            ..Default::default()
        }
    }
    #[test]
    fn full_width_and_mixed_domain_boundaries() {
        use Value::{Int, Num, Uint};
        let request = typed(vec![
            Int(i64::MIN),
            Int(-1),
            Uint(0),
            Num(0.5),
            Uint(1),
            Uint((1 << 53) + 1),
            Int(i64::MAX),
            Uint(1 << 63),
            Uint(u64::MAX),
            Num(18_446_744_073_709_551_616.0),
        ]);
        let plan = Intervals::new(&request).unwrap();
        // Exact integer oracle, scaled by two to represent the half edge.
        let edges: Vec<i128> = vec![
            i128::from(i64::MIN) * 2,
            -2,
            0,
            1,
            2,
            ((1i128 << 53) + 1) * 2,
            i128::from(i64::MAX) * 2,
            (1i128 << 63) * 2,
            i128::from(u64::MAX) * 2,
            (1i128 << 64) * 2,
        ];
        let values = [
            i128::from(i64::MIN),
            -2,
            -1,
            0,
            1,
            (1 << 53) - 1,
            1 << 53,
            (1 << 53) + 1,
            i128::from(i64::MAX),
            1 << 63,
            i128::from(u64::MAX) - 1,
            i128::from(u64::MAX),
        ];
        for value in values {
            let expected = edges
                .windows(2)
                .position(|w| w[0] <= value * 2 && value * 2 < w[1]);
            if let Ok(v) = i64::try_from(value) {
                assert_eq!(plan.bucket(NumBound::I(v)), expected);
            }
            if let Ok(v) = u64::try_from(value) {
                assert_eq!(plan.bucket(NumBound::U(v)), expected);
            }
        }
        assert_eq!(plan.bucket(NumBound::F(0.5)), Some(3));
        assert_eq!(plan.bucket(NumBound::F(18_446_744_073_709_551_616.0)), None);
        // Legacy double bounds still require exact integer comparisons.
        let legacy = RangeFacetField {
            column: "value".into(),
            edges: vec![
                0.0,
                9_223_372_036_854_775_808.0,
                18_446_744_073_709_551_616.0,
            ],
            ..Default::default()
        };
        let plan = Intervals::new(&legacy).unwrap();
        assert_eq!(plan.bucket(NumBound::I(i64::MAX)), Some(0));
        assert_eq!(plan.bucket(NumBound::U(u64::MAX)), Some(1));
    }
    #[test]
    fn malformed_edges_refuse_instead_of_changing_intervals() {
        use Value::{Int, Num, Uint};
        let bad = vec![
            typed(vec![]),
            typed(vec![Uint(1)]),
            typed(vec![Uint(2), Uint(1)]),
            typed(vec![Int(0), Uint(0)]),
            typed(vec![Num(-0.0), Num(0.0)]),
            typed(vec![Uint(1), Num(1.0)]),
            typed(vec![Num(f64::NAN), Int(1)]),
            typed(vec![Int(1), Num(f64::INFINITY)]),
        ];
        for req in bad {
            assert_eq!(
                Intervals::new(&req).err().unwrap().code(),
                tonic::Code::InvalidArgument
            );
        }
        let base = typed(vec![Uint(0), Uint(1)]);
        let mut bad = base.clone();
        bad.typed_edges[0].value = None;
        assert!(Intervals::new(&bad).is_err());
        let mut bad = base.clone();
        bad.typed_edges[1].exclusive = true;
        assert!(Intervals::new(&bad).is_err());
        let mut bad = base;
        bad.edges = vec![0.0, 1.0];
        assert!(Intervals::new(&bad).is_err());
    }
    #[test]
    fn merges_validate_every_interval_and_unknown_child_and_overflow() {
        let req = typed(vec![Value::Uint(u64::MAX - 1), Value::Uint(u64::MAX)]);
        let plan = Intervals::new(&req).unwrap();
        let good = plan.response(vec![3], true);
        assert_eq!(good.buckets[0].from, good.buckets[0].to); // display is insufficient
        let absent = plan.response(vec![0], false);
        let nested = merge(
            std::slice::from_ref(&req),
            &[vec![good.clone()], vec![absent.clone()]],
            false,
        )
        .unwrap();
        let root = merge(
            std::slice::from_ref(&req),
            &[nested, vec![good.clone()]],
            true,
        )
        .unwrap();
        assert_eq!(root[0].buckets[0].count, 6);
        assert!(
            !merge(std::slice::from_ref(&req), &[vec![absent.clone()]], false).unwrap()[0].known
        );
        assert!(merge(std::slice::from_ref(&req), &[vec![absent]], true).is_err());
        let mut bad = Vec::new();
        let mut child = good.clone();
        child.column = "wrong".into();
        bad.push(child);
        let mut child = good.clone();
        child.key = "wrong".into();
        bad.push(child);
        let mut child = good.clone();
        child.known = false;
        bad.push(child);
        let mut child = good.clone();
        child.buckets.clear();
        bad.push(child);
        let mut child = good.clone();
        child.buckets[0].typed_from = None;
        bad.push(child);
        let mut child = good.clone();
        child.buckets[0].typed_to.as_mut().unwrap().value = Some(Value::Uint(u64::MAX - 1));
        bad.push(child);
        let mut child = good.clone();
        child.buckets[0].to = f64::NAN;
        bad.push(child);
        for child in bad {
            for require in [false, true] {
                assert_eq!(
                    merge(std::slice::from_ref(&req), &[vec![child.clone()]], require)
                        .unwrap_err()
                        .code(),
                    tonic::Code::FailedPrecondition
                );
            }
        }
        let huge = plan.response(vec![u64::MAX], true);
        assert_eq!(
            merge(&[req], &[vec![huge], vec![good]], true)
                .unwrap_err()
                .code(),
            tonic::Code::OutOfRange
        );
    }
}
