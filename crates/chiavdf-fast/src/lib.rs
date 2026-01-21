#![deny(missing_docs)]
#![deny(unreachable_pub)]

//! Minimal Rust wrapper around a fast chiavdf C API.

/// Public API for this crate.
pub mod api;

mod ffi;

pub use api::{
    ChiavdfFastError, prove_one_weso_fast, prove_one_weso_fast_streaming,
    prove_one_weso_fast_streaming_getblock_opt,
    prove_one_weso_fast_streaming_getblock_opt_with_progress,
    prove_one_weso_fast_streaming_with_progress, prove_one_weso_fast_with_progress,
};
