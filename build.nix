{ pkgs, callPackage }:
callPackage ./Cargo.nix {
  inherit pkgs;
  # defaultCrateOverrides = pkgs.defaultCrateOverrides // {};
}