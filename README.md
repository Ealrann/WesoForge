# WesoForge

https://weso.forgeros.fr

WesoForge is a client for **bluebox compaction**. It leases compaction work from the backend, computes compact VDF proof witnesses, and submits results back.

Under the hood it relies on a slightly modified `chiavdf` to improve parallelism for bluebox compaction:
https://github.com/Ealrann/chiavdf

## CLI options

See `--help` for the full list. Common options:

- `-p, --parallel <N>` (env `BBR_PARALLEL_PROOFS`, default = logical CPU count, range = 1..512)
- `--no-tui` (env `BBR_NO_TUI=1`) for plain logs (recommended for large `--parallel` values)
- `-m, --mem <BUDGET>` (env `BBR_MEM_BUDGET`, default = `128MB`)

## Runtime notes

### Linux

- The CLI binary is dynamically linked (you may need GMP + C++ runtime depending on your distro).
- The GUI uses the system WebView on Linux (WebKitGTK); depending on distro/version you may need to install the corresponding runtime packages.

### macOS

- The CLI binary is dynamically linked against GMP and the C++ runtime (installed via Homebrew or system libraries).
- The GUI is distributed as a DMG containing the `.app` bundle.

## Build (from source)

### Linux (CLI, release)

Builds the production client (default backend = `https://weso.forgeros.fr/`) and writes a versioned artifact under `dist/`.

Dependencies (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y git curl build-essential clang libgmp-dev libboost-all-dev gawk
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build:

```bash
./build-cli.sh
```

### Linux (GUI AppImage, release)

Builds the AppImage and writes a versioned artifact under `dist/`.

Additional dependencies (Debian/Ubuntu):

```bash
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
sudo apt install -y nodejs
corepack enable pnpm
cargo install tauri-cli
sudo apt install -y pkg-config libgtk-3-dev libsoup-3.0-dev libwebkit2gtk-4.1-dev
```

Build:

```bash
./build-gui.sh
```

Support build (release, but with devtools enabled):

```bash
SUPPORT_DEVTOOLS=1 ./build-gui.sh
```

Notes:
- Requires `pnpm` (for the Svelte frontend).
- Requires the Tauri CLI (`cargo tauri`) to be installed (e.g. `cargo install tauri-cli`).
- Building the GUI needs the usual Tauri/Linux build deps (GTK/WebKitGTK development packages); package names vary per distro.

### macOS (CLI, release)

Builds the production client (default backend = `https://weso.forgeros.fr/`) and writes a versioned artifact under `dist/`.

Dependencies (Homebrew):

```bash
brew install gmp boost llvm
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build:

```bash
./build-cli.sh
```

### macOS (GUI DMG, release)

Builds the DMG and writes a versioned artifact under `dist/`.

Additional dependencies (Homebrew):

```bash
brew install node
corepack enable pnpm
cargo install tauri-cli
```

Build:

```bash
./build-gui.sh
```

Support build (release, but with devtools enabled):

```bash
SUPPORT_DEVTOOLS=1 ./build-gui.sh
```


### Windows (CLI)

Prereqs:
- Rust (via rustup)
- Visual Studio 2022 (MSVC + Windows SDK)
- LLVM (for `clang-cl`) *(or set `BBR_CLANG_CL` to your `clang-cl.exe` path)*

Setup:

```powershell
git submodule update --init --recursive
cd chiavdf
git clone https://github.com/Chia-Network/mpir_gc_x64.git
cd ..
```

Build (release):

```powershell
powershell -ExecutionPolicy Bypass -File .\\build-cli.ps1
```

The artifact is written under `dist/` (and includes the required `mpir*.dll` runtime files).

### Windows (GUI, portable ZIP)

Prereqs:
- Rust (via rustup)
- Node.js `20.19+` (or `22.12+`) + `pnpm`
- Visual Studio 2022 (MSVC + Windows SDK)
- LLVM (for `clang-cl`) *(or set `BBR_CLANG_CL` to your `clang-cl.exe` path)*
- Tauri CLI: `cargo install tauri-cli`

Setup:

```powershell
git submodule update --init --recursive
cd chiavdf
git clone https://github.com/Chia-Network/mpir_gc_x64.git
cd ..
```

Build + package (portable ZIP):

```powershell
powershell -ExecutionPolicy Bypass -File .\\build-gui.ps1
```

The artifact is written under `dist/WesoForge-gui_Windows_<version>_<arch>.zip`.

## Run

### GUI (Linux)

```bash
./dist/WesoForge-gui_Linux_<version>_<arch>.AppImage
```

### GUI (macOS)

- Open `dist/WesoForge-gui_macOS_<version>_<arch>.dmg`
- Drag `WesoForge.app` to Applications (or run directly from the DMG)

### GUI (Windows, portable)

- Unzip `dist/WesoForge-gui_Windows_<version>_<arch>.zip`
- Run `WesoForge\\WesoForge.exe`

### CLI (Linux / macOS)

```bash
./dist/WesoForge-cli_<OS>_<version>_<arch>
```

## Development (local backend)

- CLI: `scripts/dev_cli.sh`
- GUI: `scripts/dev_gui.sh`
