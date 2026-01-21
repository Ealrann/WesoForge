fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "linux" {
        // The chiavdf fast wrapper bundles prebuilt assembly objects that are not PIE/PIC-safe.
        // Rust defaults to PIE on many Linux distros, so we disable PIE for this binary.
        println!("cargo:rustc-link-arg-bin=bbr-client=-no-pie");
    }
}
