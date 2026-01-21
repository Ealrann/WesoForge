//! FFI bindings to the chiavdf fast C wrapper.

use std::ffi::c_void;

/// C byte buffer returned by the chiavdf fast wrapper.
#[repr(C)]
pub(crate) struct ChiavdfByteArray {
    /// Pointer to heap-allocated bytes (owned by chiavdf).
    pub(crate) data: *mut u8,
    /// Length of the buffer in bytes.
    pub(crate) length: usize,
}

#[repr(C)]
pub(crate) struct ChiavdfBatchJob {
    pub(crate) y_ref_s: *const u8,
    pub(crate) y_ref_s_size: usize,
    pub(crate) num_iterations: u64,
}

pub(crate) type ProgressCallback = unsafe extern "C" fn(iters_done: u64, user_data: *mut c_void);

unsafe extern "C" {
    pub(crate) fn chiavdf_set_bucket_memory_budget_bytes(bytes: u64);
    pub(crate) fn chiavdf_get_last_streaming_parameters(
        out_k: *mut u32,
        out_l: *mut u32,
        out_tuned: *mut bool,
    ) -> bool;
    pub(crate) fn chiavdf_set_enable_streaming_stats(enable: bool);
    pub(crate) fn chiavdf_get_last_streaming_stats(
        out_checkpoint_total_ns: *mut u64,
        out_checkpoint_event_total_ns: *mut u64,
        out_finalize_total_ns: *mut u64,
        out_checkpoint_calls: *mut u64,
        out_bucket_updates: *mut u64,
    ) -> bool;

    pub(crate) fn chiavdf_prove_one_weso_fast(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_with_progress(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
        progress_interval: u64,
        progress_cb: Option<ProgressCallback>,
        progress_user_data: *mut c_void,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        y_ref_s: *const u8,
        y_ref_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming_with_progress(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        y_ref_s: *const u8,
        y_ref_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
        progress_interval: u64,
        progress_cb: Option<ProgressCallback>,
        progress_user_data: *mut c_void,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming_getblock_opt(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        y_ref_s: *const u8,
        y_ref_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming_getblock_opt_with_progress(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        y_ref_s: *const u8,
        y_ref_s_size: usize,
        discriminant_size_bits: usize,
        num_iterations: u64,
        progress_interval: u64,
        progress_cb: Option<ProgressCallback>,
        progress_user_data: *mut c_void,
    ) -> ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        discriminant_size_bits: usize,
        jobs: *const ChiavdfBatchJob,
        job_count: usize,
    ) -> *mut ChiavdfByteArray;

    pub(crate) fn chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
        challenge_hash: *const u8,
        challenge_size: usize,
        x_s: *const u8,
        x_s_size: usize,
        discriminant_size_bits: usize,
        jobs: *const ChiavdfBatchJob,
        job_count: usize,
        progress_interval: u64,
        progress_cb: Option<ProgressCallback>,
        progress_user_data: *mut c_void,
    ) -> *mut ChiavdfByteArray;

    pub(crate) fn chiavdf_free_byte_array_batch(arrays: *mut ChiavdfByteArray, count: usize);

    pub(crate) fn chiavdf_free_byte_array(array: ChiavdfByteArray);
}
