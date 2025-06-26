use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");

    // Länkning till statiska libzstd.a
    println!("cargo:rustc-link-search=native=../zstd-local/lib");
    println!("cargo:rustc-link-lib=static=zstd");

    // Skapa bindningar med rätt ZSTD-makron
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg("-DZSTD_MULTITHREAD")
        .clang_arg("-Izstd/lib")
        .clang_arg("-Izstd/lib/common")
        .clang_arg("-Izstd/lib/compress")
        .clang_arg("-Izstd/lib/decompress")
        .clang_arg("-Izstd/lib/dictBuilder")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
