{ stdenv, pkgs, callPackage }:
let
  lib = pkgs.lib;
  iconvOptional = lib.optionals stdenv.isDarwin [pkgs.libiconv];
in import ./Cargo.nix {
  inherit pkgs;
  defaultCrateOverrides = pkgs.defaultCrateOverrides // {
    "pcap" = attrs: {
      LD_LIBRARY_PATH = "${pkgs.libpcap}/lib";
      LD_RUN_PATH = "${pkgs.libpcap}/lib";
      buildInputs = [pkgs.libpcap];
    };
    "dht-traffic-stealer" = attrs: {
      LD_LIBRARY_PATH = "${pkgs.libpcap}/lib";
      LD_RUN_PATH = "${pkgs.libpcap}/lib";
      buildInputs = [pkgs.libpcap];
    };
    "dht-traffic-stats" = attrs: {
      LD_LIBRARY_PATH = "${pkgs.libpcap}/lib";
      LD_RUN_PATH = "${pkgs.libpcap}/lib";
      buildInputs = [pkgs.libpcap];
    };
    "magnetite" = attrs: {
      buildInputs = iconvOptional ++ [pkgs.protobuf];
      PROTOC = "${pkgs.protobuf}/bin/protoc";
    };
    "prost-build" = attrs: {
      buildInputs = iconvOptional ++ [pkgs.protobuf];
      PROTOC = "${pkgs.protobuf}/bin/protoc";
    };
  };
}