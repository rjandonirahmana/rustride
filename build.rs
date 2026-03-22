fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/ridehailing.proto",
                "proto/order.proto",
                "proto/message.proto",
                "proto/notification.proto",
                "proto/auth.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
