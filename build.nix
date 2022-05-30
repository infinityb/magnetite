{ pkgs, callPackage }:
callPackage ./Cargo.nix {
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
  };
}