//! Public API for the chiavdf fast C wrapper.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

use thiserror::Error;

use crate::ffi;

struct ProgressCtx {
    cb: *mut (dyn FnMut(u64) + Send),
}

unsafe extern "C" fn progress_trampoline(iters_done: u64, user_data: *mut c_void) {
    let ctx = unsafe { &mut *(user_data as *mut ProgressCtx) };
    let cb = unsafe { &mut *ctx.cb };
    let _ = catch_unwind(AssertUnwindSafe(|| (cb)(iters_done)));
}

/// Errors returned by [`prove_one_weso_fast`].
#[derive(Debug, Error)]
pub enum ChiavdfFastError {
    /// One or more inputs are invalid.
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),

    /// The native library failed to produce a proof.
    #[error("chiavdf fast prove failed")]
    NativeFailure,

    /// The native library returned a buffer with an unexpected length.
    #[error("unexpected result length: {0}")]
    UnexpectedLength(usize),
}

fn take_result(array: ffi::ChiavdfByteArray) -> Result<Vec<u8>, ChiavdfFastError> {
    if array.data.is_null() || array.length == 0 {
        return Err(ChiavdfFastError::NativeFailure);
    }

    // SAFETY: The native library returns a heap-allocated buffer of `length`
    // bytes. We copy it out before freeing it.
    let out = unsafe { std::slice::from_raw_parts(array.data, array.length).to_vec() };
    unsafe { ffi::chiavdf_free_byte_array(array) };

    if out.len() < 2 || out.len() % 2 != 0 {
        return Err(ChiavdfFastError::UnexpectedLength(out.len()));
    }

    Ok(out)
}

/// Compute a compact (witness_type=0) Wesolowski proof using the fast chiavdf engine.
///
/// Returns a byte buffer `y || proof` (typically 200 bytes for 1024-bit discriminants).
pub fn prove_one_weso_fast(
    challenge_hash: &[u8],
    x_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
) -> Result<Vec<u8>, ChiavdfFastError> {
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it.
    unsafe {
        take_result(ffi::chiavdf_prove_one_weso_fast(
            challenge_hash.as_ptr(),
            challenge_hash.len(),
            x_s.as_ptr(),
            x_s.len(),
            discriminant_size_bits,
            num_iterations,
        ))
    }
}

/// Compute a compact (witness_type=0) Wesolowski proof using the fast chiavdf engine.
///
/// Invokes `progress` every `progress_interval` iterations completed.
///
/// Returns a byte buffer `y || proof` (typically 200 bytes for 1024-bit discriminants).
pub fn prove_one_weso_fast_with_progress<F>(
    challenge_hash: &[u8],
    x_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
    progress_interval: u64,
    mut progress: F,
) -> Result<Vec<u8>, ChiavdfFastError>
where
    F: FnMut(u64) + Send + 'static,
{
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }
    if progress_interval == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "progress_interval must be > 0",
        ));
    }

    let cb: &mut (dyn FnMut(u64) + Send) = &mut progress;
    let mut ctx = ProgressCtx {
        cb: cb as *mut (dyn FnMut(u64) + Send),
    };

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it. The callback and context pointers
    // live for the duration of this call.
    unsafe {
        take_result(ffi::chiavdf_prove_one_weso_fast_with_progress(
            challenge_hash.as_ptr(),
            challenge_hash.len(),
            x_s.as_ptr(),
            x_s.len(),
            discriminant_size_bits,
            num_iterations,
            progress_interval,
            Some(progress_trampoline),
            std::ptr::addr_of_mut!(ctx).cast::<c_void>(),
        ))
    }
}

/// Compute a compact (witness_type=0) Wesolowski proof using the fast chiavdf engine,
/// using the known expected output `y_ref` (Trick 1 streaming mode).
///
/// Returns a byte buffer `y || proof` (typically 200 bytes for 1024-bit discriminants).
pub fn prove_one_weso_fast_streaming(
    challenge_hash: &[u8],
    x_s: &[u8],
    y_ref_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
) -> Result<Vec<u8>, ChiavdfFastError> {
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if y_ref_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("y_ref_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it.
    unsafe {
        take_result(ffi::chiavdf_prove_one_weso_fast_streaming(
            challenge_hash.as_ptr(),
            challenge_hash.len(),
            x_s.as_ptr(),
            x_s.len(),
            y_ref_s.as_ptr(),
            y_ref_s.len(),
            discriminant_size_bits,
            num_iterations,
        ))
    }
}

/// Same as [`prove_one_weso_fast_streaming`], but invokes `progress` every
/// `progress_interval` iterations completed.
pub fn prove_one_weso_fast_streaming_with_progress<F>(
    challenge_hash: &[u8],
    x_s: &[u8],
    y_ref_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
    progress_interval: u64,
    mut progress: F,
) -> Result<Vec<u8>, ChiavdfFastError>
where
    F: FnMut(u64) + Send + 'static,
{
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if y_ref_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("y_ref_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }
    if progress_interval == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "progress_interval must be > 0",
        ));
    }

    let cb: &mut (dyn FnMut(u64) + Send) = &mut progress;
    let mut ctx = ProgressCtx {
        cb: cb as *mut (dyn FnMut(u64) + Send),
    };

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it. The callback and context pointers
    // live for the duration of this call.
    unsafe {
        take_result(ffi::chiavdf_prove_one_weso_fast_streaming_with_progress(
            challenge_hash.as_ptr(),
            challenge_hash.len(),
            x_s.as_ptr(),
            x_s.len(),
            y_ref_s.as_ptr(),
            y_ref_s.len(),
            discriminant_size_bits,
            num_iterations,
            progress_interval,
            Some(progress_trampoline),
            std::ptr::addr_of_mut!(ctx).cast::<c_void>(),
        ))
    }
}

/// Same as [`prove_one_weso_fast_streaming`], but uses an optimized `GetBlock()`
/// implementation (algo 2).
pub fn prove_one_weso_fast_streaming_getblock_opt(
    challenge_hash: &[u8],
    x_s: &[u8],
    y_ref_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
) -> Result<Vec<u8>, ChiavdfFastError> {
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if y_ref_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("y_ref_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it.
    unsafe {
        take_result(ffi::chiavdf_prove_one_weso_fast_streaming_getblock_opt(
            challenge_hash.as_ptr(),
            challenge_hash.len(),
            x_s.as_ptr(),
            x_s.len(),
            y_ref_s.as_ptr(),
            y_ref_s.len(),
            discriminant_size_bits,
            num_iterations,
        ))
    }
}

/// Same as [`prove_one_weso_fast_streaming_getblock_opt`], but invokes `progress`
/// every `progress_interval` iterations completed.
pub fn prove_one_weso_fast_streaming_getblock_opt_with_progress<F>(
    challenge_hash: &[u8],
    x_s: &[u8],
    y_ref_s: &[u8],
    discriminant_size_bits: usize,
    num_iterations: u64,
    progress_interval: u64,
    mut progress: F,
) -> Result<Vec<u8>, ChiavdfFastError>
where
    F: FnMut(u64) + Send + 'static,
{
    if challenge_hash.is_empty() {
        return Err(ChiavdfFastError::InvalidInput(
            "challenge_hash must not be empty",
        ));
    }
    if x_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("x_s must not be empty"));
    }
    if y_ref_s.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("y_ref_s must not be empty"));
    }
    if discriminant_size_bits == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "discriminant_size_bits must be > 0",
        ));
    }
    if num_iterations == 0 {
        return Err(ChiavdfFastError::InvalidInput("num_iterations must be > 0"));
    }
    if progress_interval == 0 {
        return Err(ChiavdfFastError::InvalidInput(
            "progress_interval must be > 0",
        ));
    }

    let cb: &mut (dyn FnMut(u64) + Send) = &mut progress;
    let mut ctx = ProgressCtx {
        cb: cb as *mut (dyn FnMut(u64) + Send),
    };

    // SAFETY: We pass pointers + lengths for all byte slices, and we copy out
    // the returned buffer before freeing it. The callback and context pointers
    // live for the duration of this call.
    unsafe {
        take_result(
            ffi::chiavdf_prove_one_weso_fast_streaming_getblock_opt_with_progress(
                challenge_hash.as_ptr(),
                challenge_hash.len(),
                x_s.as_ptr(),
                x_s.len(),
                y_ref_s.as_ptr(),
                y_ref_s.len(),
                discriminant_size_bits,
                num_iterations,
                progress_interval,
                Some(progress_trampoline),
                std::ptr::addr_of_mut!(ctx).cast::<c_void>(),
            ),
        )
    }
}
