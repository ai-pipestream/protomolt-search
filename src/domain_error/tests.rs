//! Every domain error variant converts to exactly the status the module
//! built before conversion, and every enum stays compact.

use tonic::Code;

use super::DomainError;
use crate::cel::CelError;
use crate::derived::DerivedError;
use crate::filter::FilterError;
use crate::highlight::HighlightError;
use crate::ltr::LtrError;
use crate::mapping::MappingError;
use crate::values::ValuesError;

/// Check exact RPC code and message parity and compact domain storage.
pub(crate) fn assert_status<E>(error: E, code: tonic::Code, message: &str)
where
    E: DomainError + Clone + Eq,
    tonic::Status: From<E>,
{
    assert!(size_of::<E>() <= 24);
    assert!(size_of::<Result<(), E>>() <= 48);
    assert_eq!(error.to_string().as_bytes(), message.as_bytes());
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), code);
    assert_eq!(status.message().as_bytes(), message.as_bytes());
    assert!(status.metadata().is_empty());
    assert!(status.details().is_empty());
}

#[test]
fn filter_variants_keep_code_and_message() {
    assert_status(
        FilterError::invalid_argument("filter: refused"),
        Code::InvalidArgument,
        "filter: refused",
    );
    let empty_facet = crate::pb::FilterExpr {
        expr: Some(crate::pb::filter_expr::Expr::Facet(
            crate::pb::FacetPredicate {
                column: "tag".into(),
                values: vec![],
            },
        )),
    };
    let refused = crate::filter::validate_filter(&empty_facet).unwrap_err();
    assert_status(
        refused,
        Code::InvalidArgument,
        "filter facet predicate on \"tag\": at least one value; membership in the empty set \
         is a predicate nobody meant to send",
    );
}

#[test]
fn cel_variants_keep_code_and_message() {
    assert_status(
        CelError::invalid_argument("filter: refused"),
        Code::InvalidArgument,
        "filter: refused",
    );
    assert_status(
        CelError::from(FilterError::invalid_argument("from filter")),
        Code::InvalidArgument,
        "from filter",
    );
    let refused = crate::cel::compile_value("").unwrap_err();
    assert_status(
        refused,
        Code::InvalidArgument,
        "filter: empty value expression",
    );
}

#[test]
fn values_variants_keep_code_and_message() {
    assert_status(
        ValuesError::invalid_argument("projection: refused"),
        Code::InvalidArgument,
        "projection: refused",
    );
    assert_status(
        ValuesError::failed_precondition("contract moved"),
        Code::FailedPrecondition,
        "contract moved",
    );
}

#[test]
fn ltr_variants_keep_code_and_message() {
    assert_status(
        LtrError::invalid_argument("refused"),
        Code::InvalidArgument,
        "refused",
    );
    assert_status(
        LtrError::failed_precondition("signal absent"),
        Code::FailedPrecondition,
        "signal absent",
    );
}

#[test]
fn derived_variants_keep_code_and_message() {
    assert_status(
        DerivedError::invalid_argument("forged"),
        Code::InvalidArgument,
        "forged",
    );
    assert_status(
        DerivedError::failed_precondition("unlogged"),
        Code::FailedPrecondition,
        "unlogged",
    );
}

#[test]
fn mapping_variants_keep_code_and_message() {
    assert_status(
        MappingError::invalid_argument("plan: refused"),
        Code::InvalidArgument,
        "plan: refused",
    );
    assert_status(
        MappingError::internal("plan path is absent from schema"),
        Code::Internal,
        "plan path is absent from schema",
    );
    assert_status(
        MappingError::resource_exhausted("document projection exceeds max_rows"),
        Code::ResourceExhausted,
        "document projection exceeds max_rows",
    );
    assert_status(
        MappingError::failed_precondition("invalid binding"),
        Code::FailedPrecondition,
        "invalid binding",
    );
    let refused = crate::mapping::derive_plan(b"", "pkg.Missing").unwrap_err();
    let message = refused.message().to_owned();
    assert!(message.starts_with("plan: "), "{message}");
    assert_status(refused, Code::InvalidArgument, &message);
}

#[test]
fn highlight_variants_keep_code_and_message() {
    assert_status(
        HighlightError::invalid_argument("refused"),
        Code::InvalidArgument,
        "refused",
    );
    assert_status(
        HighlightError::internal("off a boundary"),
        Code::Internal,
        "off a boundary",
    );
    assert_status(
        HighlightError::failed_precondition("no sentences"),
        Code::FailedPrecondition,
        "no sentences",
    );
}

#[test]
fn prefixed_keeps_the_code_and_fronts_the_message() {
    assert_status(
        MappingError::resource_exhausted("chunk ordinal exceeds u32").prefixed("chunk 7: "),
        Code::ResourceExhausted,
        "chunk 7: chunk ordinal exceeds u32",
    );
    assert_status(
        DerivedError::failed_precondition("unlogged").prefixed("document 3: "),
        Code::FailedPrecondition,
        "document 3: unlogged",
    );
}

#[test]
fn every_domain_error_is_one_tag_and_one_pointer_pair() {
    for size in [
        size_of::<FilterError>(),
        size_of::<CelError>(),
        size_of::<ValuesError>(),
        size_of::<LtrError>(),
        size_of::<DerivedError>(),
        size_of::<MappingError>(),
        size_of::<HighlightError>(),
    ] {
        assert!(size <= 24, "{size}");
    }
}
