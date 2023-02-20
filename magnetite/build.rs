fn main() {
    #[cfg(feature = "with-fuse")]
    tonic_build::compile_protos("../api/fuse/fuse.proto").unwrap();
}
