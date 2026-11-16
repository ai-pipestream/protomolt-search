//! One shape for the transport-free errors of the middle-tier modules.
//!
//! Compilers and evaluators refuse with a fully formatted message and the
//! gRPC code it maps to; they never build a `tonic::Status` themselves. The
//! conversion to `Status` happens once, at the RPC funnel, and reproduces
//! the code and message byte for byte. Each error stays a tag plus one
//! pointer pair, so `Result<_, Error>` is small where `Status` is not.
//!
//! A module that calls a helper still returning `Status` names a
//! `foreign` variant: the status rides boxed and converts back unchanged.

/// The rewrap every domain error shares, for callers that add context
/// without leaving the domain.
pub trait DomainError: std::error::Error + Sized {
    /// The same error with `prefix` put in front of its message.
    fn prefixed(self, prefix: &str) -> Self;
}

/// Declares a module's domain error enum.
///
/// Each `Code => constructor` line declares one `Box<str>` message variant
/// named after the `tonic::Code` it converts to, and its crate-private
/// constructor. An optional trailing `foreign: Variant` adds a boxed
/// `tonic::Status` variant that converts back to the identical status.
macro_rules! domain_error {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($code:ident => $ctor:ident),+ $(,)?
        }
        $(foreign: $foreign:ident)?
    ) => {
        $(#[$meta])*
        pub enum $name {
            $($code(Box<str>),)+
            $($foreign(Box<tonic::Status>),)?
        }

        const _: () = assert!(::core::mem::size_of::<$name>() <= 24);

        impl $name {
            $(
                pub(crate) fn $ctor(message: impl Into<Box<str>>) -> Self {
                    Self::$code(message.into())
                }
            )+

            $(
                /// Carries a status from a helper outside this module.
                pub(crate) fn foreign(status: tonic::Status) -> Self {
                    Self::$foreign(Box::new(status))
                }
            )?

            /// The message the converted status carries.
            pub fn message(&self) -> &str {
                match self {
                    $(Self::$code(message) => message,)+
                    $(Self::$foreign(status) => status.message(),)?
                }
            }

            /// The gRPC code the converted status carries.
            pub fn code(&self) -> tonic::Code {
                match self {
                    $(Self::$code(_) => tonic::Code::$code,)+
                    $(Self::$foreign(status) => status.code(),)?
                }
            }
        }

        impl $crate::domain_error::DomainError for $name {
            fn prefixed(self, prefix: &str) -> Self {
                match self {
                    $(Self::$code(message) => Self::$code(format!("{prefix}{message}").into()),)+
                    $(Self::$foreign(status) => Self::foreign(tonic::Status::new(
                        status.code(),
                        format!("{prefix}{}", status.message()),
                    )),)?
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.message())
            }
        }

        impl ::std::error::Error for $name {}

        impl From<$name> for tonic::Status {
            fn from(error: $name) -> Self {
                let code = error.code();
                match error {
                    $($name::$code(message) => Self::new(code, String::from(message)),)+
                    $($name::$foreign(status) => *status,)?
                }
            }
        }
    };
}

pub(crate) use domain_error;

/// Asserts that `error` converts to a status with exactly `code` and
/// `message`, that its accessors agree with that status, and that it and
/// its `Result` stay within 48 bytes.
#[cfg(test)]
pub(crate) fn assert_status<E>(error: E, code: tonic::Code, message: &str)
where
    E: DomainError,
    tonic::Status: From<E>,
{
    assert!(size_of::<E>() <= 48);
    assert!(size_of::<Result<(), E>>() <= 48);
    assert_eq!(error.to_string().as_bytes(), message.as_bytes());
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), code);
    assert_eq!(status.message().as_bytes(), message.as_bytes());
    assert!(status.metadata().is_empty());
    assert!(status.details().is_empty());
}

#[cfg(test)]
mod tests;
