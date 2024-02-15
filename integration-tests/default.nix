{ callPackage, ... }: {
  magnetite-tracker = callPackage ./magnetite-tracker-test.nix {};
  magnetite-seeder = callPackage ./magnetite-seeder-test.nix {};
}