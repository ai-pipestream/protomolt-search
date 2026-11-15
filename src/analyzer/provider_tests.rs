use super::*;

#[test]
fn native_provider_runs_the_product_specs_with_utf8_offsets() {
    let folded =
        analyze_document_native("😀 Running Rodríguez running", Some(&body_spec())).unwrap();
    let body = &folded.fields[0];
    assert_eq!(body.length, 4);
    assert_eq!(
        body.terms,
        vec![
            ("😀".to_string(), 1, vec![(0, 4)]),
            ("run".to_string(), 2, vec![(5, 12), (24, 31)]),
            ("rodriguez".to_string(), 1, vec![(13, 23)]),
        ]
    );

    let cased = analyze_document_native("Running running", Some(&cased_body_spec())).unwrap();
    assert_eq!(
        cased.fields[0].terms,
        vec![
            ("Run".to_string(), 1, vec![(0, 7)]),
            ("run".to_string(), 1, vec![(8, 15)]),
        ]
    );
}

#[test]
fn turbo_inprocess_provider_projects_dehyphenated_utf8_offsets_and_nlp_layers() {
    let mut pdf_spec = body_spec();
    pdf_spec.char_filters.insert(0, CHAR_FILTER_DEHYPHENATE);
    let text = "The re-\nentered order in Paris cost $120.00.";
    let doc = analyze_document_turbo(
        text,
        Some(&pdf_spec),
        SessionLayers {
            quality: true,
            geography: true,
            entities: true,
            sentences: true,
            dual_cased: true,
            nlp_columns: false,
        },
    )
    .unwrap();
    let body = &doc.fields[0];
    body.check_positions().expect("valid ascending positions");
    body.check_sentences()
        .expect("sentences cover every occurrence");
    let reenter = body
        .terms
        .iter()
        .find(|(t, _, _)| t == "reenter")
        .expect("dehyphenated joined term present");
    assert_eq!(
        &text[reenter.2[0].0 as usize..reenter.2[0].1 as usize],
        "re-\nentered"
    );
    assert!(doc.cased.is_some(), "dual_cased twin populated in one pass");
    assert!(doc.quality.is_some(), "quality layer populated in-process");
    assert_eq!(
        doc.geography.as_ref().map(|g| g.country.as_str()),
        Some("FR"),
        "BundledGazetteer resolves Paris -> FR in-process"
    );
}

#[test]
fn native_provider_refuses_unknown_and_server_defined_contracts() {
    let no_spec = analyze_document_native("court", None).unwrap_err();
    assert_eq!(no_spec.code(), tonic::Code::FailedPrecondition);
    assert!(no_spec.message().contains("explicit AnalysisSpec"));

    let mut unsupported = body_spec();
    unsupported.char_filters.push(CHAR_FILTER_NFKC);
    let error = analyze_document_native("court", Some(&unsupported)).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error
        .message()
        .contains("does not implement normalizer step 12"));
}

#[tokio::test]
async fn native_stream_uses_the_same_analysis_as_unary() {
    let mut stream = AnalyzeStream::open(NATIVE_ANALYSIS_BACKEND, Some(&body_spec()))
        .await
        .unwrap();
    let submit = stream.submitter();
    submit.submit(41, "running runs run").await.unwrap();
    drop(submit);
    stream.finish();
    let (sequence, streamed) = stream.next().await.unwrap().unwrap();
    assert_eq!(sequence, 41);
    assert_eq!(
        streamed.unwrap(),
        analyze_document_native("running runs run", Some(&body_spec())).unwrap()
    );
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn native_stream_keeps_document_errors_local() {
    let mut stream = AnalyzeStream::open(NATIVE_ANALYSIS_BACKEND, Some(&body_spec()))
        .await
        .unwrap();
    let submit = stream.submitter();
    submit.submit(1, "running").await.unwrap();
    submit.submit(2, "").await.unwrap();
    submit.submit(3, "appeals").await.unwrap();
    drop(submit);
    stream.finish();

    assert!(stream.next().await.unwrap().unwrap().1.is_ok());
    let error = stream.next().await.unwrap().unwrap().1.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(stream.next().await.unwrap().unwrap().1.is_ok());
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn native_provider_refuses_sidecar_only_capabilities() {
    let embedding = embed_text(NATIVE_ANALYSIS_BACKEND, "court")
        .await
        .unwrap_err();
    assert_eq!(embedding.code(), tonic::Code::FailedPrecondition);

    let layer = AnalyzeStream::open_with_vocab(
        NATIVE_ANALYSIS_BACKEND,
        Some(&body_spec()),
        None,
        SessionLayers {
            dual_cased: false,
            sentences: false,
            quality: true,
            geography: false,
            entities: false,
            nlp_columns: false,
        },
    )
    .await
    .err()
    .expect("quality is not served by minimal native backend");
    assert_eq!(layer.code(), tonic::Code::FailedPrecondition);
}
