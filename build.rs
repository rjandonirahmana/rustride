fn main() {
    tonic_build::configure()
        .build_server(true) // skip generate server code
        .build_client(true)
        .compile(&["proto/ridehailing.proto"], &["proto"])
        .unwrap_or_else(|e| println!("cargo:warning=proto build failed: {e}"));
    // ↑ unwrap_or_else bukan unwrap — jadi error tidak stop build
}
