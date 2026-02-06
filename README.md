# WesoForge

https://weso.forgeros.fr

WesoForge is a bluebox compaction client. It leases compaction work, computes compact VDF proofs, and submits results.

Under the hood it uses a modified `chiavdf` focused on parallel compaction performance:
https://github.com/Ealrann/chiavdf

## TL;DR Quick Start

1. Run a prebuilt CLI binary
   - Linux: `./dist/WesoForge-cli_Linux_<version>_<arch>`
   - macOS: `./dist/WesoForge-cli_macOS_<version>_<arch>`
   - Windows: `.\dist\WesoForge-cli_Windows_<version>_<arch>.exe`
2. Build Linux CLI (release): `./build-cli.sh`
3. Build Windows CLI (release): `powershell -ExecutionPolicy Bypass -File .\build-cli.ps1`

## Table of Contents

- [CLI Options](#cli-options)
- [Build Linux](#build-linux)
- [Build macOS](#build-macos)
- [Build Windows](#build-windows)
- [Run](#run)
- [Development](#development)
- [Advanced Docs](#advanced-docs)

## CLI Options

Default work mode is `group`.

### Basic

- `-p, --parallel <N>` (env: `BBR_PARALLEL_PROOFS`, default: logical CPU count, range: `1..=512`)
- `--mode <proof|group>` (env: `BBR_MODE`, default: `group`)
- `--no-tui` (env: `BBR_NO_TUI=1`) for plain logs
- `-m, --mem <BUDGET>` (env: `BBR_MEM_BUDGET`, default: `128MB`)

### Advanced

- `--pin <off|l3>` (env: `BBR_PIN`, Linux-only affinity policy)
- `--bench` (runs local benchmark with current `--mode` and `-p`)
- `--backend-url <URL>` (env: `BBR_BACKEND_URL`)

## Build Linux

Full instructions (CLI + GUI): `docs/build-linux.md`

Quick commands:

```bash
./build-cli.sh
./build-gui.sh
```

## Build macOS

Full instructions (CLI + GUI): `docs/build-macos.md`

Quick commands:

```bash
./build-cli.sh
./build-gui.sh
```

## Build Windows

Full instructions (prereqs, one-time setup, CLI build, GUI build): `docs/build-windows.md`

Quick commands:

```powershell
git submodule update --init --recursive
cd chiavdf
git clone https://github.com/Chia-Network/mpir_gc_x64.git
cd ..
powershell -ExecutionPolicy Bypass -File .\build-cli.ps1
powershell -ExecutionPolicy Bypass -File .\build-gui.ps1
```

## Run

- Linux CLI: `./dist/WesoForge-cli_Linux_<version>_<arch>`
- macOS CLI: `./dist/WesoForge-cli_macOS_<version>_<arch>`
- Linux GUI: `./dist/WesoForge-gui_Linux_<version>_<arch>.AppImage`
- macOS GUI: open `dist/WesoForge-gui_macOS_<version>_<arch>.dmg`, then drag `WesoForge.app` to Applications
- Windows GUI: unzip `dist/WesoForge-gui_Windows_<version>_<arch>.zip`, then run `WesoForge\WesoForge.exe`

## Development

- CLI (local backend): `scripts/dev_cli.sh`
- GUI (local backend): `scripts/dev_gui.sh`

## Advanced Docs

- Linux build details: `docs/build-linux.md`
- macOS build details: `docs/build-macos.md`
- Windows build details: `docs/build-windows.md`
- Windows fast-path behavior and fallback: `docs/windows-fast-path.md`
