fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/timeseries.proto"], &["proto"])
        .unwrap();
}
