# Build on Windows

This page contains full Windows build instructions for the WesoForge CLI and GUI.

## Prerequisites

CLI:

- Rust (via rustup)
- Visual Studio 2022 (MSVC + Windows SDK)
- LLVM (`clang-cl`) or set `BBR_CLANG_CL` to your `clang-cl.exe` path

GUI (in addition to CLI prerequisites):

- Node.js `20.19+` (or `22.12+`)
- `pnpm`
- Tauri CLI (`cargo install tauri-cli`)

## One-time setup

```powershell
git submodule update --init --recursive
cd chiavdf
git clone https://github.com/Chia-Network/mpir_gc_x64.git
cd ..
```

## CLI build (release)

```powershell
powershell -ExecutionPolicy Bypass -File .\build-cli.ps1
```

Output:

- `dist/WesoForge-cli_Windows_<version>_<arch>.exe`
- `dist/mpir*.dll` runtime files

## GUI build (portable ZIP)

```powershell
powershell -ExecutionPolicy Bypass -File .\build-gui.ps1
```

Output:

- `dist/WesoForge-gui_Windows_<version>_<arch>.zip`

Run:

1. Unzip the ZIP artifact
2. Launch `WesoForge\WesoForge.exe`

## Operational notes

- Windows uses the optimized fast path by default.
- Set `BBR_FORCE_WINDOWS_FALLBACK=1` to force fallback mode.
- For troubleshooting and architecture details, see `docs/windows-fast-path.md`.
