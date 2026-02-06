# Build on macOS

This page contains full macOS build instructions for the WesoForge CLI and GUI.

## CLI (release)

Builds the production client (default backend: `https://weso.forgeros.fr/`) and writes a versioned artifact under `dist/`.

Dependencies (Homebrew):

```bash
brew install gmp boost llvm
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build:

```bash
./build-cli.sh
```

Output:

- `dist/WesoForge-cli_macOS_<version>_<arch>`

Runtime note:

- The CLI is dynamically linked against GMP and the C++ runtime.

## GUI DMG (release)

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

Output:

- `dist/WesoForge-gui_macOS_<version>_<arch>.dmg`

Run:

1. Open the DMG artifact
2. Drag `WesoForge.app` to Applications (or run directly from the DMG)
