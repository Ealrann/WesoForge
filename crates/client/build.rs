fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "linux" {
        // The chiavdf fast wrapper bundles prebuilt assembly objects that are not PIE/PIC-safe.
        // Rust defaults to PIE on many Linux distros, so we disable PIE for this binary.
        println!("cargo:rustc-link-arg-bin=wesoforge=-no-pie");
    } else if target_os == "windows" {
        // chiavdf's generated assembly uses 32-bit absolute relocations in a few paths.
        // Keep the final executable link settings compatible with those objects.
        println!("cargo:rustc-link-arg-bin=wesoforge=/LARGEADDRESSAWARE:NO");
    }
}
