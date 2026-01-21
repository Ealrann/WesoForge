//! Public API for the chiavdf fast C wrapper.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use thiserror::Error;

use crate::ffi;

/// One VDF proof job input for the batch (“Trick 2”) API.
#[derive(Debug, Clone, Copy)]
pub struct ChiavdfBatchJob<'a> {
    /// Serialized expected output form (`y_ref`), typically 100 bytes.
    pub y_ref_s: &'a [u8],
    /// Target number of iterations for this proof.
    pub num_iterations: u64,
}

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

/// Parameters selected by the streaming prover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingParameters {
    /// Bucket width parameter.
    pub k: u32,
    /// Number of rows.
    pub l: u32,
    /// Whether the selection came from the memory-budget tuner.
    pub tuned: bool,
}

/// Timing counters collected by the native streaming prover (when enabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamingStats {
    /// Total time spent updating buckets at checkpoint boundaries.
    pub checkpoint_time: Duration,
    /// Total time spent handling checkpoint events (includes checkpoint_time).
    pub checkpoint_event_time: Duration,
    /// Total time spent in the streaming finalization/folding phase.
    pub finalize_time: Duration,
    /// Number of checkpoint calls processed.
    pub checkpoint_calls: u64,
    /// Number of per-bucket updates performed during checkpoint processing.
    pub bucket_updates: u64,
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

/// Set the memory budget (in bytes) used by the streaming prover parameter tuner.
///
/// This budget is per process; when running multiple worker processes, each
/// worker should set its own budget.
///
/// If `bytes` is 0, the native library falls back to its default heuristic.
pub fn set_bucket_memory_budget_bytes(bytes: u64) {
    // SAFETY: This is a simple configuration setter with no pointers.
    unsafe { ffi::chiavdf_set_bucket_memory_budget_bytes(bytes) };
}

/// Enable or disable native timing counters for the streaming prover.
///
/// Intended for benchmarking/tuning; keep disabled for normal operation.
pub fn set_enable_streaming_stats(enable: bool) {
    // SAFETY: This is a simple configuration setter with no pointers.
    unsafe { ffi::chiavdf_set_enable_streaming_stats(enable) };
}

/// Return the most recent `(k,l)` parameters selected for a streaming proof on the current thread.
///
/// Intended for debugging/benchmarking.
pub fn last_streaming_parameters() -> Option<StreamingParameters> {
    let mut k: u32 = 0;
    let mut l: u32 = 0;
    let mut tuned: bool = false;

    // SAFETY: We pass pointers to initialized scalars.
    let ok = unsafe {
        ffi::chiavdf_get_last_streaming_parameters(
            std::ptr::addr_of_mut!(k),
            std::ptr::addr_of_mut!(l),
            std::ptr::addr_of_mut!(tuned),
        )
    };

    ok.then_some(StreamingParameters { k, l, tuned })
}

/// Return timing counters for the most recent streaming proof on the current thread.
///
/// Returns `None` if timing collection is disabled or no streaming proof has been
/// computed successfully on this thread since enabling it.
pub fn last_streaming_stats() -> Option<StreamingStats> {
    let mut checkpoint_total_ns: u64 = 0;
    let mut checkpoint_event_total_ns: u64 = 0;
    let mut finalize_total_ns: u64 = 0;
    let mut checkpoint_calls: u64 = 0;
    let mut bucket_updates: u64 = 0;

    // SAFETY: We pass pointers to initialized scalars.
    let ok = unsafe {
        ffi::chiavdf_get_last_streaming_stats(
            std::ptr::addr_of_mut!(checkpoint_total_ns),
            std::ptr::addr_of_mut!(checkpoint_event_total_ns),
            std::ptr::addr_of_mut!(finalize_total_ns),
            std::ptr::addr_of_mut!(checkpoint_calls),
            std::ptr::addr_of_mut!(bucket_updates),
        )
    };

    ok.then_some(StreamingStats {
        checkpoint_time: Duration::from_nanos(checkpoint_total_ns),
        checkpoint_event_time: Duration::from_nanos(checkpoint_event_total_ns),
        finalize_time: Duration::from_nanos(finalize_total_ns),
        checkpoint_calls,
        bucket_updates,
    })
}

struct BatchResultGuard {
    ptr: *mut ffi::ChiavdfByteArray,
    count: usize,
}

impl Drop for BatchResultGuard {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated by the native batch API and must be freed
        // exactly once with `chiavdf_free_byte_array_batch`.
        unsafe { ffi::chiavdf_free_byte_array_batch(self.ptr, self.count) };
    }
}

fn take_result_batch(
    ptr: *mut ffi::ChiavdfByteArray,
    count: usize,
) -> Result<Vec<Vec<u8>>, ChiavdfFastError> {
    if ptr.is_null() || count == 0 {
        return Err(ChiavdfFastError::NativeFailure);
    }

    let guard = BatchResultGuard { ptr, count };

    // SAFETY: `ptr` points to an array of `count` `ChiavdfByteArray` entries.
    let arrays = unsafe { std::slice::from_raw_parts(guard.ptr, guard.count) };

    let mut out = Vec::with_capacity(count);
    for array in arrays {
        if array.data.is_null() || array.length == 0 {
            return Err(ChiavdfFastError::NativeFailure);
        }
        // SAFETY: The native library returns a heap-allocated buffer of `length`
        // bytes. We copy it out before freeing the batch.
        let bytes = unsafe { std::slice::from_raw_parts(array.data, array.length).to_vec() };
        if bytes.len() < 2 || bytes.len() % 2 != 0 {
            return Err(ChiavdfFastError::UnexpectedLength(bytes.len()));
        }
        out.push(bytes);
    }

    drop(guard);
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

/// Compute multiple compact (witness_type=0) Wesolowski proofs in one shared
/// squaring run (Trick 2), using:
/// - streaming bucket accumulation (Trick 1)
/// - precomputed `GetBlock()` mapping (GetBlock opt)
///
/// Returns one `y || proof` buffer per job (same format as single-job APIs).
pub fn prove_one_weso_fast_streaming_getblock_opt_batch(
    challenge_hash: &[u8],
    x_s: &[u8],
    discriminant_size_bits: usize,
    jobs: &[ChiavdfBatchJob<'_>],
) -> Result<Vec<Vec<u8>>, ChiavdfFastError> {
    prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
        challenge_hash,
        x_s,
        discriminant_size_bits,
        jobs,
        0,
        |_| {},
    )
}

/// Same as [`prove_one_weso_fast_streaming_getblock_opt_batch`], but invokes
/// `progress` every `progress_interval` squaring iterations completed.
pub fn prove_one_weso_fast_streaming_getblock_opt_batch_with_progress<F>(
    challenge_hash: &[u8],
    x_s: &[u8],
    discriminant_size_bits: usize,
    jobs: &[ChiavdfBatchJob<'_>],
    progress_interval: u64,
    mut progress: F,
) -> Result<Vec<Vec<u8>>, ChiavdfFastError>
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
    if jobs.is_empty() {
        return Err(ChiavdfFastError::InvalidInput("jobs must not be empty"));
    }
    for job in jobs {
        if job.y_ref_s.is_empty() {
            return Err(ChiavdfFastError::InvalidInput(
                "job y_ref_s must not be empty",
            ));
        }
        if job.num_iterations == 0 {
            return Err(ChiavdfFastError::InvalidInput(
                "job num_iterations must be > 0",
            ));
        }
    }

    let ffi_jobs: Vec<ffi::ChiavdfBatchJob> = jobs
        .iter()
        .map(|job| ffi::ChiavdfBatchJob {
            y_ref_s: job.y_ref_s.as_ptr(),
            y_ref_s_size: job.y_ref_s.len(),
            num_iterations: job.num_iterations,
        })
        .collect();

    let ptr = if progress_interval == 0 {
        // SAFETY: Pointers + lengths are provided for all slices and the
        // returned batch pointer is freed by `take_result_batch`.
        unsafe {
            ffi::chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch(
                challenge_hash.as_ptr(),
                challenge_hash.len(),
                x_s.as_ptr(),
                x_s.len(),
                discriminant_size_bits,
                ffi_jobs.as_ptr(),
                ffi_jobs.len(),
            )
        }
    } else {
        let cb: &mut (dyn FnMut(u64) + Send) = &mut progress;
        let mut ctx = ProgressCtx {
            cb: cb as *mut (dyn FnMut(u64) + Send),
        };
        // SAFETY: Same as above, with progress callback + context valid for the
        // duration of the call.
        unsafe {
            ffi::chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
                challenge_hash.as_ptr(),
                challenge_hash.len(),
                x_s.as_ptr(),
                x_s.len(),
                discriminant_size_bits,
                ffi_jobs.as_ptr(),
                ffi_jobs.len(),
                progress_interval,
                Some(progress_trampoline),
                std::ptr::addr_of_mut!(ctx).cast::<c_void>(),
            )
        }
    };

    take_result_batch(ptr, ffi_jobs.len())
}
