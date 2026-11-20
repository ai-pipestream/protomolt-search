//! Convert domain refusals at the RPC boundary.

use tonic::Status;

macro_rules! status_from {
    ($error:ident, $($variant:ident),+ $(,)?) => {
        impl From<$error> for Status {
            fn from(error: $error) -> Self {
                match error {
                    $($error::$variant(message) => Self::new(tonic::Code::$variant, String::from(message))),+
                }
            }
        }
    };
}

use crate::cel::CelError;
status_from!(CelError, InvalidArgument);

use crate::filter::FilterError;
status_from!(FilterError, InvalidArgument);

use crate::ltr::LtrError;
status_from!(LtrError, InvalidArgument, FailedPrecondition);

use crate::derived::DerivedError;
status_from!(DerivedError, InvalidArgument, FailedPrecondition);

use crate::mapping::MappingError;
status_from!(
    MappingError,
    InvalidArgument,
    Internal,
    FailedPrecondition,
    ResourceExhausted
);

use crate::values::ValuesError;
status_from!(ValuesError, InvalidArgument, FailedPrecondition);

use crate::highlight::HighlightError;
status_from!(
    HighlightError,
    InvalidArgument,
    Internal,
    FailedPrecondition
);
