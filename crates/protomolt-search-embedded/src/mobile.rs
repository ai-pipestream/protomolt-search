//! Stable byte-oriented ABI used by the Android AAR and Apple XCFramework.

#[cfg(any(target_os = "android", test))]
mod android;
mod bridge;
mod config;
mod ffi;
mod ops;
#[cfg(test)]
mod tests;

pub use self::ffi::MobileBuffer;
