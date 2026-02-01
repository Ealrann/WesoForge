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
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("compile_asm.cpp").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        chiavdf_src.join("refcode").join("lzcnt.c").display()
    );

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_os == "windows" {
        build_windows_fallback(&manifest_dir, &chiavdf_dir, &chiavdf_src);
        return;
    }
    // The full chiavdf fast engine uses x86 intrinsics and assembly; use the
    // portable "slow" fallback on macOS ARM (Apple Silicon) instead.
    if target_os == "macos" && target_arch == "aarch64" {
        build_macos_arm_fallback(&manifest_dir, &chiavdf_src);
        return;
    }

    // GMP (and gmpxx) may be in a non-default location (e.g. Homebrew on macOS).
    // Pass include path via CXXFLAGS so the compiler can find <gmpxx.h> and <gmp.h>.
    let (gmp_cflags, gmp_link_search) = detect_gmp_paths();
    let mut make_env: Vec<(String, String)> = Vec::new();
    let mut cxxflags = gmp_cflags.clone().unwrap_or_default();
    if let Some(ref boost) = detect_boost_include() {
        if !cxxflags.is_empty() {
            cxxflags.push(' ');
        }
        cxxflags.push_str(boost);
    }
    if let Ok(ref existing) = env::var("CXXFLAGS") {
        if !cxxflags.is_empty() {
            cxxflags.push(' ');
        }
        cxxflags.push_str(existing);
    }
    if !cxxflags.is_empty() {
        make_env.push(("CXXFLAGS".to_string(), cxxflags));
    }

    let mut make_cmd = Command::new("make");
    make_cmd.current_dir(&chiavdf_src);
    for (k, v) in &make_env {
        make_cmd.env(k, v);
    }
    let status = make_cmd
        .arg("-f")
        .arg("Makefile.vdf-client")
        // Let `make` use its incremental rebuild logic.
        .arg("fastlib")
        .arg("PIC=1")
        .arg("LTO=")
        .status()
        .expect("failed to run make to build chiavdf fast library");

    if !status.success() {
        panic!("chiavdf fast library build failed (exit code: {status})");
    }

    println!("cargo:rustc-link-search=native={}", chiavdf_src.display());
    if let Some(ref lib_dir) = gmp_link_search {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }
    println!("cargo:rustc-link-lib=static=chiavdf_fastc");

    // chiavdf depends on GMP and pthread.
    println!("cargo:rustc-link-lib=gmpxx");
    println!("cargo:rustc-link-lib=gmp");
    println!("cargo:rustc-link-lib=pthread");

    // We link C++ objects, so we need the C++ standard library.
    // Keep it simple: this project currently targets Linux.
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

fn build_windows_fallback(manifest_dir: &PathBuf, chiavdf_dir: &PathBuf, chiavdf_src: &PathBuf) {
    let fallback_cpp = manifest_dir.join("native").join("chiavdf_fast_fallback.cpp");
    println!("cargo:rerun-if-changed={}", fallback_cpp.display());

    // The chiavdf repository expects the MPIR (GMP-compatible) Windows bundle to
    // live at `chiavdf/mpir_gc_x64`.
    let mpir_dir = chiavdf_dir.join("mpir_gc_x64");
    let mpir_lib = mpir_dir.join("mpir.lib");
    if !mpir_lib.exists() {
        panic!(
            "mpir.lib not found at {}. Ensure chiavdf/mpir_gc_x64 is present (see chiavdf's pyproject.toml windows build instructions).",
            mpir_lib.display()
        );
    }

    // The chiavdf sources use GNU/Clang builtins (e.g. __builtin_clzll) even
    // on Windows. Compile this fallback with clang-cl for compatibility.
    //
    // Prefer an explicit path if the user configured one; otherwise fall back
    // to the default winget install location.
    let clang_cl = env::var("BBR_CLANG_CL").unwrap_or_else(|_| {
        let default = PathBuf::from(r"C:\Program Files\LLVM\bin\clang-cl.exe");
        if default.exists() {
            default.to_string_lossy().to_string()
        } else {
            "clang-cl".to_string()
        }
    });

    let mut build = cc::Build::new();
    build.cpp(true);
    build.compiler(clang_cl);
    build.flag("/std:c++17");
    build.flag("/EHsc");
    build.flag("/O2");
    build.define("_CRT_SECURE_NO_WARNINGS", None);
    build.include(chiavdf_src);
    build.include(&mpir_dir);
    build.file(fallback_cpp);
    build.file(chiavdf_src.join("refcode").join("lzcnt.c"));
    build.compile("chiavdf_fastc");

    // Link against MPIR (GMP-compatible) import library.
    println!("cargo:rustc-link-search=native={}", mpir_dir.display());
    println!("cargo:rustc-link-lib=mpir");
}

/// Build the portable "slow" fallback on macOS ARM (Apple Silicon). The full
/// chiavdf fast engine uses x86 intrinsics/assembly and is not available there.
fn build_macos_arm_fallback(manifest_dir: &PathBuf, chiavdf_src: &PathBuf) {
    let fallback_cpp = manifest_dir.join("native").join("chiavdf_fast_fallback.cpp");
    let lzcnt_c = chiavdf_src.join("refcode").join("lzcnt.c");
    println!("cargo:rerun-if-changed={}", fallback_cpp.display());
    println!("cargo:rerun-if-changed={}", lzcnt_c.display());

    let (gmp_cflags, gmp_link_search) = detect_gmp_paths();

    // C++ fallback implementation (must not include lzcnt.c: see below).
    let mut build_cpp = cc::Build::new();
    build_cpp.cpp(true);
    build_cpp.flag("-std=c++17");
    build_cpp.flag("-O2");
    build_cpp.define("VDF_MODE", "0");
    build_cpp.include(chiavdf_src);
    if let Some(ref cflags) = gmp_cflags {
        for flag in cflags.split_whitespace() {
            if flag.starts_with("-I") {
                let path = flag.strip_prefix("-I").unwrap_or(flag);
                build_cpp.include(path);
            }
        }
    }
    build_cpp.file(fallback_cpp);
    build_cpp.compile("chiavdf_fastc");

    // lzcnt.c must be compiled as C (not C++) so has_lzcnt_hard, lzcnt64_soft,
    // lzcnt64_hard keep C linkage and match Reducer.h's extern "C" declarations.
    cc::Build::new()
        .file(lzcnt_c)
        .flag("-O2")
        .compile("lzcnt");

    if let Some(ref lib_dir) = gmp_link_search {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }
    println!("cargo:rustc-link-lib=gmpxx");
    println!("cargo:rustc-link-lib=gmp");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=c++");
}

/// Detect GMP include path so the compiler can find `<gmp.h>` and `<gmpxx.h>`.
/// Returns (cflags, optional lib dir for link search). Both are None if system defaults work.
fn detect_gmp_paths() -> (Option<String>, Option<PathBuf>) {
    // Prefer pkg-config (works on macOS with Homebrew and many Linux distros).
    for pkg in ["gmpxx", "gmp"] {
        if let Ok(output) = Command::new("pkg-config").args(["--cflags", pkg]).output() {
            if output.status.success() {
                let cflags = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !cflags.is_empty() {
                    // Optionally get lib dir for link search (e.g. Homebrew's path).
                    let lib_dir = Command::new("pkg-config")
                        .args(["--variable=libdir", pkg])
                        .output()
                        .ok()
                        .filter(|o| o.status.success())
                        .and_then(|o| {
                            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                            if s.is_empty() {
                                None
                            } else {
                                Some(PathBuf::from(s))
                            }
                        });
                    return (Some(cflags), lib_dir);
                }
            }
        }
    }

    // On macOS, Homebrew installs GMP to /opt/homebrew (Apple Silicon) or /usr/local (Intel).
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        if let Ok(output) = Command::new("brew").args(["--prefix", "gmp"]).output() {
            if output.status.success() {
                let prefix = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !prefix.is_empty() {
                    let prefix_path = PathBuf::from(&prefix);
                    let include = prefix_path.join("include");
                    if include.join("gmpxx.h").exists() {
                        return (
                            Some(format!("-I{}", include.display())),
                            Some(prefix_path.join("lib")),
                        );
                    }
                }
            }
        }
        // Fallback: common Homebrew paths (avoid calling brew if not in PATH).
        for prefix in ["/opt/homebrew", "/usr/local"] {
            let prefix_path = PathBuf::from(prefix);
            let gmpxx = prefix_path.join("include").join("gmpxx.h");
            if gmpxx.exists() {
                return (
                    Some(format!("-I{}/include", prefix)),
                    Some(prefix_path.join("lib")),
                );
            }
        }
    }

    (None, None)
}

/// Boost include path on macOS (Homebrew). Full chiavdf build needs <boost/asio.hpp>.
fn detect_boost_include() -> Option<String> {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return None;
    }
    if let Ok(output) = Command::new("brew").args(["--prefix", "boost"]).output() {
        if output.status.success() {
            let prefix = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !prefix.is_empty() && PathBuf::from(&prefix).join("include").join("boost").join("asio.hpp").exists() {
                return Some(format!("-I{}/include", prefix));
            }
        }
    }
    for prefix in ["/opt/homebrew", "/usr/local"] {
        if PathBuf::from(prefix).join("include").join("boost").join("asio.hpp").exists() {
            return Some(format!("-I{}/include", prefix));
        }
    }
    None
}
