#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <cfenv>
#include <limits>
#include <mutex>
#include <vector>

// This is a Windows-friendly fallback implementation of the "chiavdf fast" C API.
//
// The Linux implementation relies on the full fast chiavdf engine (including
// generated assembly and a GNU Make-based build). On Windows, that toolchain is
// not available by default.
//
// Instead, we implement the same API surface using chiavdf's portable "slow"
// proving code. This is functional but not optimized for throughput.

#include "verifier.h"
#include "alloc.hpp"
#include "prover_slow.h"

typedef struct {
    uint8_t* data;
    size_t length;
} ChiavdfByteArray;

typedef struct {
    const uint8_t* y_ref_s;
    size_t y_ref_s_size;
    uint64_t num_iterations;
} ChiavdfBatchJob;

typedef void (*ChiavdfProgressCallback)(uint64_t iters_done, void* user_data);

namespace {
std::once_flag init_once;
std::atomic<uint64_t> bucket_memory_budget_bytes(0);
std::atomic<bool> streaming_stats_enabled(false);

struct LastStreamingParameters {
    uint32_t k = 0;
    uint32_t l = 0;
    bool tuned = false;
    bool set = false;
};

thread_local LastStreamingParameters last_streaming_parameters;

struct LastStreamingStats {
    uint64_t checkpoint_total_ns = 0;
    uint64_t checkpoint_event_total_ns = 0;
    uint64_t finalize_total_ns = 0;
    uint64_t checkpoint_calls = 0;
    uint64_t bucket_updates = 0;
    bool set = false;
};

thread_local LastStreamingStats last_streaming_stats;

ChiavdfByteArray empty_result() { return ChiavdfByteArray{nullptr, 0}; }

uint64_t saturating_add_u64(uint64_t lhs, uint64_t rhs) {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

void free_byte_array_batch_internal(ChiavdfByteArray* arrays, size_t count) {
    if (arrays == nullptr) {
        return;
    }
    for (size_t idx = 0; idx < count; ++idx) {
        delete[] arrays[idx].data;
        arrays[idx].data = nullptr;
        arrays[idx].length = 0;
    }
    delete[] arrays;
}

struct BatchProgressContext {
    uint64_t completed_before = 0;
    ChiavdfProgressCallback progress_cb = nullptr;
    void* progress_user_data = nullptr;
};

void batch_progress_trampoline(uint64_t iters_done, void* user_data) {
    auto* ctx = static_cast<BatchProgressContext*>(user_data);
    if (ctx == nullptr || ctx->progress_cb == nullptr) {
        return;
    }
    ctx->progress_cb(saturating_add_u64(ctx->completed_before, iters_done), ctx->progress_user_data);
}

void init_chiavdf_runtime() {
    init_gmp();
    fesetround(FE_TOWARDZERO);
}

ChiavdfByteArray prove_one_weso_slow(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    const uint8_t* y_ref_s,
    size_t y_ref_s_size,
    bool check_y_ref,
    size_t discriminant_size_bits,
    uint64_t num_iterations,
    uint64_t progress_interval,
    ChiavdfProgressCallback progress_cb,
    void* progress_user_data) {
    if (challenge_hash == nullptr || challenge_size == 0 || x_s == nullptr || x_s_size == 0) {
        return empty_result();
    }
    if (num_iterations == 0 || discriminant_size_bits == 0) {
        return empty_result();
    }

    try {
        std::call_once(init_once, init_chiavdf_runtime);

        std::vector<uint8_t> challenge_hash_bytes(challenge_hash, challenge_hash + challenge_size);
        integer D = CreateDiscriminant(challenge_hash_bytes, static_cast<int>(discriminant_size_bits));
        integer L = root(-D, 4);

        form x = DeserializeForm(D, x_s, x_s_size);

        PulmarkReducer reducer;
        form y = form::from_abd(x.a, x.b, D);
        int d_bits = D.num_bits();

        int k = 0;
        int l = 0;
        ApproximateParameters(num_iterations, l, k);
        if (k <= 0) {
            k = 1;
        }
        if (l <= 0) {
            l = 1;
        }
        int kl = k * l;
        if (kl <= 0) {
            return empty_result();
        }

        last_streaming_parameters.k = static_cast<uint32_t>(k);
        last_streaming_parameters.l = static_cast<uint32_t>(l);
        last_streaming_parameters.tuned = false;
        last_streaming_parameters.set = true;
        last_streaming_stats = LastStreamingStats{};

        const uint64_t size_vec = (num_iterations + static_cast<uint64_t>(kl) - 1) / static_cast<uint64_t>(kl);
        std::vector<form> intermediates(static_cast<size_t>(size_vec));
        form* cursor = intermediates.data();

        for (uint64_t i = 0; i < num_iterations; i++) {
            if ((i % static_cast<uint64_t>(kl)) == 0) {
                *cursor = y;
                ++cursor;
            }

            nudupl_form(y, y, D, L);
            reducer.reduce(y);

            if (progress_cb != nullptr && progress_interval != 0) {
                const uint64_t done = i + 1;
                if (done == num_iterations || (done % progress_interval) == 0) {
                    progress_cb(done, progress_user_data);
                }
            }
        }

        form proof = GenerateWesolowski(
            y,
            x,
            D,
            reducer,
            intermediates,
            num_iterations,
            static_cast<uint64_t>(k),
            static_cast<uint64_t>(l));

        std::vector<unsigned char> y_serialized = SerializeForm(y, d_bits);
        std::vector<unsigned char> proof_serialized = SerializeForm(proof, d_bits);
        if (y_serialized.empty() || proof_serialized.empty()) {
            return empty_result();
        }

        if (check_y_ref) {
            if (y_ref_s == nullptr || y_ref_s_size == 0) {
                return empty_result();
            }
            if (y_serialized.size() != y_ref_s_size) {
                return empty_result();
            }
            if (!std::equal(y_serialized.begin(), y_serialized.end(), y_ref_s)) {
                return empty_result();
            }
        }

        const size_t total = y_serialized.size() + proof_serialized.size();
        uint8_t* out = new uint8_t[total];
        std::copy(y_serialized.begin(), y_serialized.end(), out);
        std::copy(proof_serialized.begin(), proof_serialized.end(), out + y_serialized.size());
        return ChiavdfByteArray{out, total};
    } catch (...) {
        return empty_result();
    }
}
} // namespace

extern "C" void chiavdf_set_bucket_memory_budget_bytes(uint64_t bytes) {
    bucket_memory_budget_bytes.store(bytes, std::memory_order_relaxed);
}

extern "C" void chiavdf_set_enable_streaming_stats(bool enable) {
    streaming_stats_enabled.store(enable, std::memory_order_relaxed);
    last_streaming_stats = LastStreamingStats{};
}

extern "C" bool chiavdf_get_last_streaming_parameters(uint32_t* out_k, uint32_t* out_l, bool* out_tuned) {
    if (out_k == nullptr || out_l == nullptr || out_tuned == nullptr) {
        return false;
    }
    if (!last_streaming_parameters.set) {
        return false;
    }
    *out_k = last_streaming_parameters.k;
    *out_l = last_streaming_parameters.l;
    *out_tuned = last_streaming_parameters.tuned;
    return true;
}

extern "C" bool chiavdf_get_last_streaming_stats(
    uint64_t* out_checkpoint_total_ns,
    uint64_t* out_checkpoint_event_total_ns,
    uint64_t* out_finalize_total_ns,
    uint64_t* out_checkpoint_calls,
    uint64_t* out_bucket_updates) {
    (void)out_checkpoint_total_ns;
    (void)out_checkpoint_event_total_ns;
    (void)out_finalize_total_ns;
    (void)out_checkpoint_calls;
    (void)out_bucket_updates;
    // The fallback does not implement streaming stats.
    return false;
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations) {
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        /*y_ref_s=*/nullptr,
        /*y_ref_s_size=*/0,
        /*check_y_ref=*/false,
        discriminant_size_bits,
        num_iterations,
        /*progress_interval=*/0,
        /*progress_cb=*/nullptr,
        /*progress_user_data=*/nullptr);
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast_with_progress(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations,
    uint64_t progress_interval,
    ChiavdfProgressCallback progress_cb,
    void* progress_user_data) {
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        /*y_ref_s=*/nullptr,
        /*y_ref_s_size=*/0,
        /*check_y_ref=*/false,
        discriminant_size_bits,
        num_iterations,
        progress_interval,
        progress_cb,
        progress_user_data);
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast_streaming(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    const uint8_t* y_ref_s,
    size_t y_ref_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations) {
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        y_ref_s,
        y_ref_s_size,
        /*check_y_ref=*/true,
        discriminant_size_bits,
        num_iterations,
        /*progress_interval=*/0,
        /*progress_cb=*/nullptr,
        /*progress_user_data=*/nullptr);
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast_streaming_with_progress(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    const uint8_t* y_ref_s,
    size_t y_ref_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations,
    uint64_t progress_interval,
    ChiavdfProgressCallback progress_cb,
    void* progress_user_data) {
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        y_ref_s,
        y_ref_s_size,
        /*check_y_ref=*/true,
        discriminant_size_bits,
        num_iterations,
        progress_interval,
        progress_cb,
        progress_user_data);
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast_streaming_getblock_opt(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    const uint8_t* y_ref_s,
    size_t y_ref_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations) {
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        y_ref_s,
        y_ref_s_size,
        /*check_y_ref=*/true,
        discriminant_size_bits,
        num_iterations,
        /*progress_interval=*/0,
        /*progress_cb=*/nullptr,
        /*progress_user_data=*/nullptr);
}

extern "C" ChiavdfByteArray chiavdf_prove_one_weso_fast_streaming_getblock_opt_with_progress(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    const uint8_t* y_ref_s,
    size_t y_ref_s_size,
    size_t discriminant_size_bits,
    uint64_t num_iterations,
    uint64_t progress_interval,
    ChiavdfProgressCallback progress_cb,
    void* progress_user_data) {
    // The fallback does not implement the GetBlock optimization; it is still
    // functionally correct.
    return prove_one_weso_slow(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        y_ref_s,
        y_ref_s_size,
        /*check_y_ref=*/true,
        discriminant_size_bits,
        num_iterations,
        progress_interval,
        progress_cb,
        progress_user_data);
}

extern "C" ChiavdfByteArray* chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    size_t discriminant_size_bits,
    const ChiavdfBatchJob* jobs,
    size_t job_count,
    uint64_t progress_interval,
    ChiavdfProgressCallback progress_cb,
    void* progress_user_data) {
    if (challenge_hash == nullptr || challenge_size == 0 || x_s == nullptr || x_s_size == 0) {
        return nullptr;
    }
    if (discriminant_size_bits == 0 || jobs == nullptr || job_count == 0) {
        return nullptr;
    }

    ChiavdfByteArray* out_arrays = nullptr;
    try {
        out_arrays = new ChiavdfByteArray[job_count];
        for (size_t idx = 0; idx < job_count; ++idx) {
            out_arrays[idx] = empty_result();
        }

        uint64_t completed_iters = 0;
        for (size_t idx = 0; idx < job_count; ++idx) {
            const ChiavdfBatchJob& job = jobs[idx];
            if (job.y_ref_s == nullptr || job.y_ref_s_size == 0 || job.num_iterations == 0) {
                free_byte_array_batch_internal(out_arrays, job_count);
                return nullptr;
            }

            BatchProgressContext progress_ctx;
            progress_ctx.completed_before = completed_iters;
            progress_ctx.progress_cb = progress_cb;
            progress_ctx.progress_user_data = progress_user_data;
            const bool use_progress = progress_cb != nullptr && progress_interval != 0;

            out_arrays[idx] = prove_one_weso_slow(
                challenge_hash,
                challenge_size,
                x_s,
                x_s_size,
                job.y_ref_s,
                job.y_ref_s_size,
                /*check_y_ref=*/true,
                discriminant_size_bits,
                job.num_iterations,
                progress_interval,
                use_progress ? batch_progress_trampoline : nullptr,
                use_progress ? static_cast<void*>(&progress_ctx) : nullptr);

            if (out_arrays[idx].data == nullptr || out_arrays[idx].length == 0) {
                free_byte_array_batch_internal(out_arrays, job_count);
                return nullptr;
            }

            completed_iters = saturating_add_u64(completed_iters, job.num_iterations);
        }

        return out_arrays;
    } catch (...) {
        free_byte_array_batch_internal(out_arrays, job_count);
        return nullptr;
    }
}

extern "C" ChiavdfByteArray* chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch(
    const uint8_t* challenge_hash,
    size_t challenge_size,
    const uint8_t* x_s,
    size_t x_s_size,
    size_t discriminant_size_bits,
    const ChiavdfBatchJob* jobs,
    size_t job_count) {
    return chiavdf_prove_one_weso_fast_streaming_getblock_opt_batch_with_progress(
        challenge_hash,
        challenge_size,
        x_s,
        x_s_size,
        discriminant_size_bits,
        jobs,
        job_count,
        /*progress_interval=*/0,
        /*progress_cb=*/nullptr,
        /*progress_user_data=*/nullptr);
}

extern "C" void chiavdf_free_byte_array_batch(ChiavdfByteArray* arrays, size_t count) {
    free_byte_array_batch_internal(arrays, count);
}

extern "C" void chiavdf_free_byte_array(ChiavdfByteArray array) { delete[] array.data; }
