{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    # nativeBuildInputs is usually what you want -- tools you need to run
    nativeBuildInputs = with pkgs.buildPackages; [
        openssl.dev
        openssl
        pkg-config
        protobuf
	crate2nix
    ];
}
