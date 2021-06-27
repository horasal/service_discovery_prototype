fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../protos");
    tonic_build::configure()
        .build_server(false)
        .compile(
            &["../protos/discovery.proto", "../protos/metadata.proto"],
            &["../protos"],
        )
        .unwrap();
    tonic_build::configure()
        .build_client(false)
        .compile(&["../protos/heartbeat.proto"], &["../protos"])
        .unwrap();
}
