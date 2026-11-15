use super::wire::{
    analyzed_from_mock, doc_geography, doc_quality, MockAnalyzeResponse, WireGeoLocation,
    WireNoiseSpan, WireRegionVote, WireSpan, WireTermVector, WIRE_OFFSET_UTF16,
};
use super::SessionLayers;

fn noise_span(start: i32, end: i32, score: f64) -> WireNoiseSpan {
    WireNoiseSpan {
        span: Some(WireSpan { start, end }),
        severity: "gibberish".to_string(),
        score,
    }
}

#[test]
fn noise_chars_is_the_union_not_the_sum() {
    let response = MockAnalyzeResponse {
        noise: vec![
            noise_span(30, 32, 0.2),
            noise_span(15, 25, 0.4),
            noise_span(10, 20, 0.3),
            noise_span(15, 25, 0.4),
            noise_span(5, 5, 0.9),
        ],
        ..Default::default()
    };
    let q = doc_quality(&response);
    assert_eq!(q.noise_chars, 17, "union of [10,25) and [30,32)");
    assert_eq!(q.noise, 0.9, "worst score wins even from an empty span");
    assert_eq!(q.artifacts, 0);
}

#[test]
fn non_finite_noise_scores_are_ignored() {
    let response = MockAnalyzeResponse {
        noise: vec![noise_span(0, 4, f64::NAN), noise_span(4, 8, 0.5)],
        ..Default::default()
    };
    let q = doc_quality(&response);
    assert_eq!(q.noise, 0.5);
    assert_eq!(q.noise_chars, 8, "the NaN finding's SPAN still counts");
}

fn location(start: i32, confidence: f64, country: &str) -> WireGeoLocation {
    WireGeoLocation {
        span: Some(WireSpan {
            start,
            end: start + 5,
        }),
        name: "Somewhere".to_string(),
        country_code: country.to_string(),
        latitude: f64::from(start),
        longitude: -f64::from(start),
        confidence,
    }
}

#[test]
fn geography_picks_best_confidence_first_on_ties() {
    let response = MockAnalyzeResponse {
        locations: vec![
            location(0, 0.9, "FR"),
            location(10, 0.9, "DE"),
            location(20, 0.4, "US"),
        ],
        regions: vec![
            WireRegionVote {
                country_code: "DE".to_string(),
                share: 0.6,
            },
            WireRegionVote {
                country_code: "FR".to_string(),
                share: 0.4,
            },
        ],
        ..Default::default()
    };
    let g = doc_geography(&response);
    assert_eq!(g.point, Some((0.0, 0.0)), "tie keeps the first mention");
    assert_eq!(g.confidence, 0.9);
    assert_eq!(
        g.country, "DE",
        "the country is the aggregate vote, not the winning mention's"
    );
}

#[test]
fn non_finite_confidence_cannot_win() {
    let response = MockAnalyzeResponse {
        locations: vec![location(0, f64::NAN, "FR"), location(10, 0.3, "DE")],
        ..Default::default()
    };
    assert_eq!(doc_geography(&response).point, Some((10.0, -10.0)));

    let only_nan = MockAnalyzeResponse {
        locations: vec![location(0, f64::NAN, "FR")],
        ..Default::default()
    };
    assert_eq!(doc_geography(&only_nan).point, None);
}

#[test]
fn no_locations_reduce_to_absence() {
    let g = doc_geography(&MockAnalyzeResponse::default());
    assert_eq!(g.point, None);
    assert_eq!(g.country, "");
}

#[test]
fn a_clean_response_measures_zero() {
    let q = doc_quality(&MockAnalyzeResponse::default());
    assert_eq!(q.noise, 0.0);
    assert_eq!(q.noise_chars, 0);
    assert_eq!(q.artifacts, 0);
}

#[test]
fn zero_length_terms_are_dropped_at_the_boundary() {
    let tv = |term: &str, tf: i32| WireTermVector {
        term: term.to_string(),
        frequency: tf,
        occurrences: vec![WireSpan { start: 0, end: 5 }],
    };
    let with_empty = MockAnalyzeResponse {
        term_vectors: vec![tv("court", 2), tv("", 3), tv("appeal", 1)],
        ..Default::default()
    };
    let without = MockAnalyzeResponse {
        term_vectors: vec![tv("court", 2), tv("appeal", 1)],
        ..Default::default()
    };
    assert_eq!(
        analyzed_from_mock(with_empty, SessionLayers::default()).unwrap(),
        analyzed_from_mock(without, SessionLayers::default()).unwrap(),
    );
}

#[test]
fn search_refuses_a_sidecar_that_violates_its_utf8_storage_request() {
    let response = MockAnalyzeResponse {
        offset_unit: WIRE_OFFSET_UTF16,
        ..Default::default()
    };
    let error = analyzed_from_mock(response, SessionLayers::default()).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("ambiguous persisted spans"));
}
