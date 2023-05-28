{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        inherit system;
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            bazelisk
            bazel-buildtools

            binutils
            coreutils-full
            curlFull

            git
            gnutar
            perl

            which
            nixFlakes
          ];
        };

        formatter = pkgs.alejandra;
      }
    );
}
