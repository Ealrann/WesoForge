use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=BBR_CHIAVDF_DIR");
    println!("cargo:rerun-if-env-changed=BBR_FORCE_WINDOWS_FALLBACK");
    println!("cargo:rerun-if-env-changed=BBR_CLANG_CL");

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
        let force_windows_fallback = env_flag("BBR_FORCE_WINDOWS_FALLBACK");

        if force_windows_fallback {
            println!(
                "cargo:warning=BBR_FORCE_WINDOWS_FALLBACK=1 set; using Windows fallback implementation."
            );
            build_windows_fallback(&manifest_dir, &chiavdf_dir, &chiavdf_src);
        } else {
            build_windows_fast_path(&chiavdf_dir, &chiavdf_src);
        }
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

    let mpir_dir = windows_mpir_dir(chiavdf_dir);
    let clang_cl = detect_clang_cl();

    // The chiavdf sources use GNU/Clang builtins (e.g. __builtin_clzll) even
    // on Windows. Compile this fallback with clang-cl for compatibility.
    let mut build_cpp = cc::Build::new();
    build_cpp.cpp(true);
    build_cpp.compiler(&clang_cl);
    build_cpp.flag("/std:c++17");
    build_cpp.flag("/EHsc");
    build_cpp.flag("/O2");
    build_cpp.warnings(false);
    build_cpp.define("_CRT_SECURE_NO_WARNINGS", None);
    build_cpp.include(chiavdf_src);
    build_cpp.include(&mpir_dir);
    build_cpp.file(fallback_cpp);
    build_cpp.compile("chiavdf_fastc");

    // Keep lzcnt compiled as C so C linkage matches the chiavdf headers.
    let mut build_c = cc::Build::new();
    build_c.compiler(&clang_cl);
    build_c.flag("/O2");
    build_c.define("_CRT_SECURE_NO_WARNINGS", None);
    build_c.include(chiavdf_src);
    build_c.include(&mpir_dir);
    build_c.file(chiavdf_src.join("refcode").join("lzcnt.c"));
    build_c.compile("lzcnt");

    // Link against MPIR (GMP-compatible) import library.
    println!("cargo:rustc-link-search=native={}", mpir_dir.display());
    println!("cargo:rustc-link-lib=mpir");
    // The imported chiavdf assembly uses absolute 32-bit relocations.
    // Keep Windows link settings compatible with that model for now.
    println!("cargo:rustc-link-arg=/LARGEADDRESSAWARE:NO");
}

fn build_windows_fast_path(chiavdf_dir: &PathBuf, chiavdf_src: &PathBuf) {
    let fast_wrapper_cpp = chiavdf_src.join("c_bindings").join("fast_wrapper.cpp");
    let windows_compat_cpp = PathBuf::from("native").join("chiavdf_fast_windows_stubs.cpp");
    println!("cargo:rerun-if-changed={}", fast_wrapper_cpp.display());
    println!("cargo:rerun-if-changed={}", windows_compat_cpp.display());

    let mpir_dir = windows_mpir_dir(chiavdf_dir);
    let clang_cl = detect_clang_cl();
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let asm_objects = build_windows_asm_objects(&clang_cl, chiavdf_src, &mpir_dir, &out_dir);

    // Phase 4: link the fast-wrapper path with real assembly objects on Windows.
    let mut build_cpp = cc::Build::new();
    build_cpp.cpp(true);
    build_cpp.compiler(&clang_cl);
    build_cpp.flag("/std:c++17");
    build_cpp.flag("/EHsc");
    build_cpp.flag("/O2");
    build_cpp.flag("/W0");
    build_cpp.flag("/clang:-Wno-deprecated-literal-operator");
    build_cpp.warnings(false);
    build_cpp.define("_CRT_SECURE_NO_WARNINGS", None);
    build_cpp.define("CHIAVDF_SKIP_BOOST_ASIO", Some("1"));
    build_cpp.define("CHIAVDF_DISABLE_TEST_ASM", Some("1"));
    build_cpp.include(chiavdf_src);
    build_cpp.include(&mpir_dir);
    build_cpp.file(fast_wrapper_cpp);
    build_cpp.file(windows_compat_cpp);
    for obj in asm_objects {
        build_cpp.object(obj);
    }
    build_cpp.compile("chiavdf_fastc");

    let mut build_c = cc::Build::new();
    build_c.compiler(&clang_cl);
    build_c.flag("/O2");
    build_c.define("_CRT_SECURE_NO_WARNINGS", None);
    build_c.include(chiavdf_src);
    build_c.include(&mpir_dir);
    build_c.file(chiavdf_src.join("refcode").join("lzcnt.c"));
    build_c.compile("lzcnt");

    println!("cargo:rustc-link-search=native={}", mpir_dir.display());
    println!("cargo:rustc-link-lib=mpir");
    // Needed when linking this crate's own test binaries on Windows fast path.
    println!("cargo:rustc-link-arg=/LARGEADDRESSAWARE:NO");
}

fn build_windows_asm_objects(
    clang_cl: &str,
    chiavdf_src: &Path,
    mpir_dir: &Path,
    out_dir: &Path,
) -> Vec<PathBuf> {
    let asm_sources = ["asm_compiled.s", "avx2_asm_compiled.s", "avx512_asm_compiled.s"];
    ensure_windows_asm_sources(clang_cl, chiavdf_src, mpir_dir, out_dir, &asm_sources);
    let mut objects = Vec::with_capacity(asm_sources.len());

    for asm_name in asm_sources {
        let source_path = chiavdf_src.join(asm_name);
        println!("cargo:rerun-if-changed={}", source_path.display());
        let source = fs::read_to_string(&source_path).unwrap_or_else(|err| {
            panic!("failed to read {}: {err}", source_path.display());
        });

        let normalized = normalize_asm_for_windows(&source);
        let normalized_path = out_dir.join(format!("{asm_name}.windows.s"));
        fs::write(&normalized_path, normalized).unwrap_or_else(|err| {
            panic!("failed to write normalized asm {}: {err}", normalized_path.display());
        });

        let object_path = out_dir.join(format!("{asm_name}.obj"));
        let status = Command::new(clang_cl)
            .arg("/nologo")
            .arg("/c")
            .arg(&normalized_path)
            .arg(format!("/Fo{}", object_path.display()))
            .status()
            .unwrap_or_else(|err| {
                panic!(
                    "failed to invoke clang-cl for {}: {err}",
                    normalized_path.display()
                )
            });

        if !status.success() {
            panic!(
                "failed to assemble {} with clang-cl (exit code: {status})",
                source_path.display()
            );
        }

        objects.push(object_path);
    }

    objects
}

fn ensure_windows_asm_sources(
    clang_cl: &str,
    chiavdf_src: &Path,
    mpir_dir: &Path,
    out_dir: &Path,
    asm_sources: &[&str],
) {
    let missing_sources: Vec<&str> = asm_sources
        .iter()
        .copied()
        .filter(|name| !chiavdf_src.join(name).exists())
        .collect();
    if missing_sources.is_empty() {
        return;
    }

    println!(
        "cargo:warning=missing chiavdf asm sources ({}); regenerating via compile_asm.cpp",
        missing_sources.join(", ")
    );

    let compile_asm_cpp = chiavdf_src.join("compile_asm.cpp");
    let compile_asm_exe = out_dir.join("compile_asm_windows.exe");
    let builtins_lib = detect_clang_rt_builtins(clang_cl).unwrap_or_else(|| {
        panic!(
            "failed to locate clang runtime builtins for {} (needed to link compile_asm.cpp)",
            clang_cl
        )
    });

    let status = Command::new(clang_cl)
        .arg("/nologo")
        .arg("/std:c++17")
        .arg("/EHsc")
        .arg("/O2")
        .arg("/W0")
        .arg("/clang:-Wno-deprecated-literal-operator")
        .arg(format!("/I{}", chiavdf_src.display()))
        .arg(format!("/I{}", mpir_dir.display()))
        .arg(&compile_asm_cpp)
        .arg(format!("/Fe{}", compile_asm_exe.display()))
        .arg("/link")
        .arg(format!("/LIBPATH:{}", mpir_dir.display()))
        .arg("mpir.lib")
        .arg(&builtins_lib)
        .status()
        .unwrap_or_else(|err| {
            panic!(
                "failed to invoke clang-cl for {}: {err}",
                compile_asm_cpp.display()
            )
        });
    if !status.success() {
        panic!(
            "failed to compile {} (exit code: {status})",
            compile_asm_cpp.display()
        );
    }

    let generate_targets: [&[&str]; 3] = [&[], &["avx2"], &["avx512"]];
    for args in generate_targets {
        let mut cmd = Command::new(&compile_asm_exe);
        let existing_path = env::var_os("PATH").unwrap_or_default();
        let mut runtime_path = OsString::from(mpir_dir.as_os_str());
        runtime_path.push(";");
        runtime_path.push(existing_path);
        cmd.current_dir(chiavdf_src)
            .env("PATH", runtime_path)
            .args(args);
        let status = cmd.status().unwrap_or_else(|err| {
            panic!(
                "failed to run {} with args {:?}: {err}",
                compile_asm_exe.display(),
                args
            )
        });
        if !status.success() {
            panic!(
                "failed to generate asm sources via {} with args {:?} (exit code: {status})",
                compile_asm_exe.display(),
                args
            );
        }
    }

    let still_missing: Vec<&str> = asm_sources
        .iter()
        .copied()
        .filter(|name| !chiavdf_src.join(name).exists())
        .collect();
    if !still_missing.is_empty() {
        panic!(
            "asm generation finished but required sources are still missing: {}",
            still_missing.join(", ")
        );
    }
}

fn normalize_asm_for_windows(source: &str) -> String {
    source
        .replace(".text 1", ".section .rdata,\"dr\"")
        .replace("CMOVEQ", "CMOVE")
        .replace("OFFSET FLAT:", "OFFSET ")
}

fn windows_mpir_dir(chiavdf_dir: &PathBuf) -> PathBuf {
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
    mpir_dir
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

fn env_flag(name: &str) -> bool {
    match env::var(name) {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
        }
        Err(_) => false,
    }
}

fn detect_clang_cl() -> String {
    env::var("BBR_CLANG_CL").unwrap_or_else(|_| {
        let default = PathBuf::from(r"C:\Program Files\LLVM\bin\clang-cl.exe");
        if default.exists() {
            default.to_string_lossy().to_string()
        } else {
            "clang-cl".to_string()
        }
    })
}

fn detect_clang_rt_builtins(clang_cl: &str) -> Option<PathBuf> {
    if let Ok(output) = Command::new(clang_cl).arg("--print-resource-dir").output() {
        if output.status.success() {
            let resource_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !resource_dir.is_empty() {
                let candidate = PathBuf::from(resource_dir)
                    .join("lib")
                    .join("windows")
                    .join("clang_rt.builtins-x86_64.lib");
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    let clang_path = PathBuf::from(clang_cl);
    let llvm_root = clang_path.parent().and_then(|bin| bin.parent())?;
    let clang_lib_root = llvm_root.join("lib").join("clang");
    let versions = fs::read_dir(&clang_lib_root).ok()?;
    let mut version_dirs: Vec<PathBuf> = versions
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| path.is_dir())
        .collect();
    version_dirs.sort();
    version_dirs.reverse();

    for version_dir in version_dirs {
        let candidate = version_dir
            .join("lib")
            .join("windows")
            .join("clang_rt.builtins-x86_64.lib");
        if candidate.exists() {
            return Some(candidate);
        }
    }

    None
}
