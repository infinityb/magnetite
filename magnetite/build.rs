fn main() {
    tonic_build::prost::compile_protos("../api/fuse/fuse.proto").unwrap();
}
