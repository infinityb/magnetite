{
    system ? builtins.currentSystem,
}:
let
  nixpkgsSrc = (builtins.fetchTarball {
      # nix-prefetch-url --unpack "<URL>"
      url = "https://github.com/NixOS/nixpkgs/archive/aaf58368e3748e22c1bb826fbdb907030f58a767.tar.gz";
      sha256 = "01w6qj7ciz00nbdky963jzcf1901vri7hck79nn7fpq07mkgndi9";
  });
  nixpkgs = import nixpkgsSrc {
    inherit system;
  };

  magnetite = nixpkgs.pkgs.callPackage ./default.nix {};
  ourPackages = {
  	inherit magnetite nixpkgsSrc;
  };
	combinedPkgs = nixpkgs.pkgs // {
		callPackage = nixpkgs.lib.callPackageWith (combinedPkgs // ourPackages);
	};

	integrationTests = combinedPkgs.callPackage ./integration-tests {};
	interactiveTesting = nixpkgs.pkgs.symlinkJoin {
		name = "magentite-interactive";
		paths = [
			magnetite.workspaceMembers.magnetite-single.build
			magnetite.workspaceMembers.magnetite-single-wrapper.build
			magnetite.workspaceMembers.magnetite-single-aggregator.build
			magnetite.workspaceMembers.magnetite-tracker.build
		];
	};
in magnetite // {
	inherit integrationTests;
	inherit interactiveTesting;
}
