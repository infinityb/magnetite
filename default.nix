{
    system ? builtins.currentSystem,
}:
let
  nixpkgsSrc = (builtins.fetchTarball {
    # nix-prefetch-url --unpack "<URL>"
    url = "https://github.com/NixOS/nixpkgs/archive/32f63574c85fbc80e4ba1fbb932cde9619bad25e.tar.gz";
    sha256 = "1qykaq7a8kmk4nd6xz4zv35yi2jwbw93p93y4wh1dwxf9h0kqjhb";
  });
  nixpkgs = import nixpkgsSrc {
    inherit system;
  };

  magnetite = nixpkgs.pkgs.callPackage ./build.nix {};
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