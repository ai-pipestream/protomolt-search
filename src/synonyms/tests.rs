use super::*;

#[test]
fn edit_distance_counts_transpositions_as_one_and_respects_the_bound() {
    assert_eq!(edit_distance("court", "court", 2), 0);
    assert_eq!(edit_distance("court", "cuort", 2), 1, "a transposition");
    assert_eq!(edit_distance("court", "cort", 2), 1, "a deletion");
    assert_eq!(edit_distance("court", "courts", 2), 1, "an insertion");
    assert_eq!(edit_distance("court", "coart", 2), 1, "a substitution");
    assert_eq!(edit_distance("court", "cuorts", 2), 2);
    assert_eq!(
        edit_distance("court", "xxxxx", 2),
        3,
        "past the bound: bound + 1"
    );
    assert_eq!(edit_distance("", "ab", 1), 2);
    assert_eq!(edit_distance("naïve", "naive", 1), 1, "chars, not bytes");
}

#[test]
fn candidates_rank_by_distance_then_df_then_term() {
    let mut entries = BTreeMap::new();
    entries.insert("court".to_string(), (50, 2));
    entries.insert("courts".to_string(), (7, 1));
    entries.insert("cort".to_string(), (7, 1));
    entries.insert("coupon".to_string(), (90, 2));
    entries.insert("cour".to_string(), (1, 1));
    let ranked = rank_candidates("cuort", &entries, 2, 10);
    let got: Vec<(&str, u32)> = ranked
        .iter()
        .map(|c| (c.term.as_str(), c.distance))
        .collect();
    // "cort" is one deletion away, like the transposition "court";
    // "courts" (a transposition and an insertion) and "cour" (two
    // deletions) are two, and df orders them.
    assert_eq!(
        got,
        vec![("court", 1), ("cort", 1), ("courts", 2), ("cour", 2)],
        "distance first, then df, then the term"
    );
    assert!(rank_candidates("court", &entries, 1, 10)
        .iter()
        .all(|c| c.term != "court"));
}

#[test]
fn rules_validate_by_shape() {
    assert!(validate_rule(&SynonymRule {
        terms: vec!["car".into()],
        to: vec![],
    })
    .is_err());
    assert!(validate_rule(&SynonymRule {
        terms: vec![],
        to: vec!["x".into()],
    })
    .is_err());
    assert!(validate_rule(&SynonymRule {
        terms: vec!["a".into(), " ".into()],
        to: vec![],
    })
    .is_err());
    assert!(validate_rule(&SynonymRule {
        terms: vec!["car".into(), "automobile".into()],
        to: vec![],
    })
    .is_ok());
}
