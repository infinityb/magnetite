with (import <nixpkgs> {});
let
	magnetite = callPackage ./build.nix {};
	internalCallPackage = path: props: pkgs.callPackage path (pkgs // {inherit magnetite; } // props);
	integrationTests = internalCallPackage ./integration-tests { callPackage = internalCallPackage; };
in magnetite // { inherit integrationTests; }