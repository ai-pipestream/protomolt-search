//! Score-stage resolution against a shard's numeric tables.

use super::*;
use crate::scorefn::{ColumnRef, Stage};

/// A resolved stage column and the bounds its values lift from.
type StageColumn = (Option<ColumnRef>, Option<(f64, f64)>);

impl Bm25Shard {
    /// Resolve parsed score stages against this shard's numeric tables and
    /// min/max bounds (`None` columns evaluate as absent/identity).
    pub(in crate::node) fn resolve_chain(
        &self,
        specs: &[ScoreStageSpec],
    ) -> crate::scorefn::ScoreChain {
        crate::scorefn::ScoreChain {
            stages: specs.iter().map(|spec| self.resolve_stage(spec)).collect(),
        }
    }

    fn resolve_stage(&self, spec: &ScoreStageSpec) -> Stage {
        let ScoreStageSpec {
            op,
            column,
            key,
            typed_map,
        } = spec;
        // A geo stage reads a geo-point column and nothing
        // else: its op carries the origin, and its bound is
        // identity, so there is no min/max to lift.
        if matches!(op, crate::scorefn::StageOp::MultGeoDecay { .. }) {
            return Stage {
                op: *op,
                column: self.geo_index(column).map(ColumnRef::Geo),
                min_max: (f64::NAN, f64::NAN),
            };
        }
        let (column, min_max) = if key.is_none() {
            self.scalar_stage_column(column)
        } else if *typed_map && self.map_integer_index(column).is_some() {
            let ci = self.map_integer_index(column).expect("checked signed map");
            let key_ord = self.map_integer_key_ord(ci, key.as_deref().unwrap());
            (
                key_ord.map(|key_ord| ColumnRef::MapIntegerKey {
                    column: ci,
                    key_ord,
                }),
                key_ord
                    .and_then(|k| self.map_integer_key_min_max(ci, k))
                    .map(int_min_max_as_f64),
            )
        } else if *typed_map && self.map_unsigned_integer_index(column).is_some() {
            let ci = self
                .map_unsigned_integer_index(column)
                .expect("checked unsigned map");
            let key_ord = self.map_unsigned_integer_key_ord(ci, key.as_deref().unwrap());
            (
                key_ord.map(|key_ord| ColumnRef::MapUnsignedIntegerKey {
                    column: ci,
                    key_ord,
                }),
                key_ord
                    .and_then(|k| self.map_unsigned_integer_key_min_max(ci, k))
                    .map(uint_min_max_as_f64),
            )
        } else {
            // Map stage: both the column and the key must
            // resolve; bounds lift from the KEY's min/max.
            let key = key.as_deref().expect("map selector is present");
            let hit = self
                .map_numeric_index(column)
                .and_then(|ci| self.map_numeric_key_ord(ci, key).map(|k| (ci, k)));
            (
                hit.map(|(ci, key_ord)| ColumnRef::MapKey {
                    column: ci,
                    key_ord,
                }),
                hit.map(|(ci, key_ord)| self.map_numeric_key_min_max(ci, key_ord)),
            )
        };
        Stage {
            op: *op,
            column,
            min_max: min_max.unwrap_or((f64::NAN, f64::NAN)),
        }
    }

    /// An unkeyed stage's column. Column names are unique across numeric
    /// families.
    fn scalar_stage_column(&self, column: &str) -> StageColumn {
        if let Some(ni) = self.numeric_index(column) {
            (Some(ColumnRef::Numeric(ni)), Some(self.numeric_min_max(ni)))
        } else if let Some(ii) = self.integer_index(column) {
            (
                Some(ColumnRef::Integer(ii)),
                Some(int_min_max_as_f64(self.integer_min_max(ii))),
            )
        } else {
            let ui = self.unsigned_integer_index(column);
            (
                ui.map(ColumnRef::UnsignedInteger),
                ui.map(|ui| uint_min_max_as_f64(self.unsigned_integer_min_max(ui))),
            )
        }
    }
}
