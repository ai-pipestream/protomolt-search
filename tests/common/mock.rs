//! Re-export of the mock analysis sidecar from the library harness (it
//! lives there so the shakedown binary can use it as a documented
//! fallback when the real native sidecar is unavailable).

pub use turbovec_search::harness::mock_analysis::{start_mock_analysis, toy_stem};
