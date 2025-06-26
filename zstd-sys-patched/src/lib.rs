#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]

// Laddar bindgen-bindningarna från OUT_DIR
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
