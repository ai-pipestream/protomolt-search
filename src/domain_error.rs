//! Compact errors for synchronous compilers and validators.

mod transport;

/// The rewrap every domain error shares, for callers that add context
/// without leaving the domain.
pub trait DomainError: std::error::Error + Sized {
    /// The same error with `prefix` put in front of its message.
    fn prefixed(self, prefix: &str) -> Self;
}

/// Declare message-carrying error variants with a common size bound.
macro_rules! domain_error {
    ($(#[$meta:meta])* pub enum $name:ident { $($kind:ident => $ctor:ident),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub enum $name { $($kind(Box<str>)),+ }

        const _: () = assert!(::core::mem::size_of::<$name>() <= 24);

        impl $name {
            $(pub(crate) fn $ctor(message: impl Into<Box<str>>) -> Self {
                Self::$kind(message.into())
            })+

            pub fn message(&self) -> &str {
                match self { $(Self::$kind(message) => message),+ }
            }
        }

        impl $crate::domain_error::DomainError for $name {
            fn prefixed(self, prefix: &str) -> Self {
                match self { $(Self::$kind(message) => Self::$kind(format!("{prefix}{message}").into())),+ }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.message())
            }
        }

        impl ::std::error::Error for $name {}
    };
}

pub(crate) use domain_error;

#[cfg(test)]
pub(crate) use tests::assert_status;

#[cfg(test)]
mod tests;
