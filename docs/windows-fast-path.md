# Windows Fast-Path Runbook

This document describes the current Windows `chiavdf` build/runtime path, the fallback escape hatch, and recovery procedures.

## Architecture

- Default Windows mode uses the optimized `chiavdf` fast path (`fast_wrapper.cpp`) with real asm objects:
  - `asm_compiled.s`
  - `avx2_asm_compiled.s`
  - `avx512_asm_compiled.s`
- Asm objects are normalized for `clang-cl` before assembly (`crates/chiavdf-fast/build.rs`):
  - `.text 1` -> `.section .rdata,"dr"`
  - `CMOVEQ` -> `CMOVE`
  - `OFFSET FLAT:` -> `OFFSET `
- Windows fallback mode compiles `crates/chiavdf-fast/native/chiavdf_fast_fallback.cpp` and keeps the same Rust API.
- Build scripts keep `/LARGEADDRESSAWARE:NO` for Windows links that include chiavdf asm.

## Operational Overrides

- `BBR_FORCE_WINDOWS_FALLBACK=1`:
  - Forces the fallback implementation on Windows.
  - This is the primary operational recovery switch.
- `BBR_CLANG_CL=<path-to-clang-cl.exe>`:
  - Overrides `clang-cl` detection.
- `BBR_CHIAVDF_DIR=<path-to-chiavdf-checkout>`:
  - Uses an external chiavdf checkout instead of the submodule.

## Troubleshooting

1. `mpir.lib not found` during build
   - Ensure `chiavdf/mpir_gc_x64/mpir.lib` exists.
   - If missing, clone MPIR bundle in `chiavdf/` as documented in `README.md`.

2. Runtime fails with missing DLL (`0xc0000135`)
   - Ensure `mpir*.dll` files are next to the packaged Windows executable (the build scripts copy them to `dist/`).

3. Linker `LNK2017 ADDR32 relocation` errors
   - This indicates the final link did not receive `/LARGEADDRESSAWARE:NO`.
   - Use the repo build scripts (`build-cli.ps1`, `build-gui.ps1`) or ensure equivalent link flags in custom build entry points.

4. `clang-cl` not found
   - Install LLVM or set `BBR_CLANG_CL`.

## Rollout and Recovery

- CI should run both Windows variants:
  - Fast default
  - Forced fallback (`BBR_FORCE_WINDOWS_FALLBACK=1`)
- If a Windows fast-path regression is detected in production:
  1. Set `BBR_FORCE_WINDOWS_FALLBACK=1` for Windows builds/deployments.
  2. Publish a patch release if needed.
  3. Investigate and fix fast path while fallback keeps service continuity.
- After fix verification (build + tests + soak), remove the temporary fallback override.
