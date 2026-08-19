fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc binary not found");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/metadata.proto");

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config
        .compile_protos(&["proto/metadata.proto"], &["proto"])
        .expect("failed to compile metadata protobuf");
}
