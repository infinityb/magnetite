fn main() {
    tonic_build::compile_protos("../api/fuse/fuse.proto").unwrap();
}
