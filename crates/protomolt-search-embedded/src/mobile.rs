//! Stable byte-oriented ABI used by the Android AAR and Apple XCFramework.

#[cfg(target_os = "android")]
mod android;
mod bridge;
mod config;
mod ffi;
mod ops;
#[cfg(test)]
mod tests;

pub use self::ffi::MobileBuffer;
