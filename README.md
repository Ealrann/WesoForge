# WesoForge

https://weso.forgeros.fr

WesoForge is a client for **bluebox compaction**. It leases compaction work from the backend, computes compact VDF proof witnesses, and submits results back.

Under the hood it relies on a slightly modified `chiavdf` to improve parallelism for bluebox compaction:
https://github.com/Ealrann/chiavdf

## What you ship

- **CLI binary**: `WesoForge-cli_<version>_<arch>`
- **GUI (Linux)**: `WesoForge-gui_<version>_<arch>.AppImage`

## Build (from source)

### CLI (release)

Builds the production client (default backend = `https://weso.forgeros.fr/`) and writes a versioned artifact under `dist/`:

```bash
./build-cli.sh
```

### GUI AppImage (release, Linux)

Builds the AppImage and writes a versioned artifact under `dist/`:

```bash
./build-gui.sh
```

Notes:
- Requires `pnpm` (for the Svelte frontend).
- Requires the Tauri CLI (`cargo tauri`) to be installed (e.g. `cargo install tauri-cli`).
- Building the GUI needs the usual Tauri/Linux build deps (GTK/WebKitGTK development packages); package names vary per distro.

## Run

### GUI

```bash
./dist/WesoForge-gui_<version>_<arch>.AppImage
```

### CLI

```bash
./dist/WesoForge-cli_<version>_<arch>
```

## CLI options

See `--help` for the full list. Common options:

- `--backend-url <URL>` (env `BBR_BACKEND_URL`)
- `-p, --parallel <N>` (env `BBR_PARALLEL_PROOFS`, default = logical CPU count)
- `--no-tui` (env `BBR_NO_TUI=1`) for plain logs (recommended for large `--parallel` values)

## Linux runtime notes

- The CLI binary is dynamically linked (you may need GMP + C++ runtime depending on your distro).
- The GUI uses the system WebView on Linux (WebKitGTK); depending on distro/version you may need to install the corresponding runtime packages.

## Development (local backend)

- CLI: `scripts/dev_cli.sh`
- GUI: `scripts/dev_gui.sh`
