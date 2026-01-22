use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=BBR_CHIAVDF_DIR");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("crate is in crates/*");

    let chiavdf_dir = env::var("BBR_CHIAVDF_DIR")
        .map(PathBuf::from)
        // Default to the `chiavdf/` git submodule.
        .unwrap_or_else(|_| {
            let submodule = repo_root.join("chiavdf");
            if submodule
                .join("src")
                .join("c_bindings")
                .join("fast_wrapper.cpp")
                .exists()
            {
                return submodule;
            }

            panic!(
                "chiavdf repo not found at {}. Run `git submodule update --init --recursive` \
or set BBR_CHIAVDF_DIR to a chiavdf checkout.",
                submodule.display()
            );
        });
    let chiavdf_src = chiavdf_dir.join("src");

    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("Makefile.vdf-client").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("vdf.h").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("callback.h").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src
            .join("c_bindings")
            .join("fast_wrapper.cpp")
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src
            .join("c_bindings")
            .join("fast_wrapper.h")
            .display()
    );

    let status = Command::new("make")
        .arg("-C")
        .arg(&chiavdf_src)
        .arg("-f")
        .arg("Makefile.vdf-client")
        // Ensure we don't reuse objects built with different flags (e.g. LTO).
        .arg("-B")
        .arg("fastlib")
        .arg("PIC=1")
        .arg("LTO=")
        .status()
        .expect("failed to run make to build chiavdf fast library");

    if !status.success() {
        panic!("chiavdf fast library build failed (exit code: {status})");
    }

    println!("cargo:rustc-link-search=native={}", chiavdf_src.display());
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("libchiavdf_fastc.a").display()
    );
    println!("cargo:rustc-link-lib=static=chiavdf_fastc");

    // chiavdf depends on GMP and pthread.
    println!("cargo:rustc-link-lib=gmpxx");
    println!("cargo:rustc-link-lib=gmp");
    println!("cargo:rustc-link-lib=pthread");

    // We link C++ objects, so we need the C++ standard library.
    // Keep it simple: this project currently targets Linux.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=c++");
    } else if target_os != "windows" {
        println!("cargo:rustc-link-lib=stdc++");
    }

    // chiavdf's generated assembly isn't PIE/PIC-safe. Rust builds PIE binaries by default
    // on many Linux distros, so disable PIE for any binary that links this crate.
    if target_os == "linux" {
        println!("cargo:rustc-link-arg=-no-pie");
    }
}
