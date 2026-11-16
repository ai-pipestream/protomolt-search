//! Statistics-epoch lifetime rotation.

use super::*;

#[test]
fn mutation_counter_exhaustion_rotates_the_lifetime_instead_of_reusing_a_version() {
    let mut state = ShardState {
        stats_epoch: u64::MAX,
        ..Default::default()
    };
    let previous = state.stats_incarnation.bytes().unwrap();
    state.advance_stats_epoch();
    assert_eq!(state.stats_epoch, 1);
    assert_ne!(state.stats_incarnation.bytes().unwrap(), previous);
    assert!(state.check_stats_epoch(1, &previous).is_err());
}
