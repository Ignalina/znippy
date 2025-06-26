use std::env;
use std::path::PathBuf;
use bindgen::EnumVariation;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");

    let libzstd_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("../zstd-local/lib");
    println!("cargo:rustc-link-search=native={}", libzstd_path.display());
    println!("cargo:rustc-link-lib=static=zstd");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg("-DZSTD_STATIC_LINKING_ONLY")
        .clang_arg("-DZSTD_MULTITHREAD")
        .clang_arg("-Izstd/lib")
        .default_enum_style(EnumVariation::Consts)
        .allowlist_function("ZSTD_.*")
        .allowlist_type("ZSTD_.*")
        .allowlist_var("ZSTD_.*")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
