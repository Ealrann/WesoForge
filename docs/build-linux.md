# Build on Linux

This page contains full Linux build instructions for the WesoForge CLI and GUI.

## CLI (release)

Builds the production client (default backend: `https://weso.forgeros.fr/`) and writes a versioned artifact under `dist/`.

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

Output:

- `dist/WesoForge-cli_Linux_<version>_<arch>`

Runtime note:

- The CLI is dynamically linked (install GMP and C++ runtime packages for your distro if needed).

## GUI AppImage (release)

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

Output:

- `dist/WesoForge-gui_Linux_<version>_<arch>.AppImage`

Runtime note:

- The GUI uses system WebKitGTK; required runtime packages vary by distro.
