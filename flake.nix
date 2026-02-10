{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        package = pkgs.pkgs.rustPlatform.buildRustPackage {
          pname = "freenet";
          version = "0.1.120";

          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          cargoBuildFlags = [
            "--package"
            "freenet"
            "--bin"
            "freenet"
          ];

          doCheck = false;

          installPhase = ''
            runHook preInstall

            mkdir -p $out/bin
            cp target/*/release/freenet $out/bin/

            runHook postInstall
          '';

          meta = with pkgs.lib; {
            homepage = "https://github.com/freenet/freenet-core";
            license = licenses.agpl3Only;
          };
        };
      in
      {
        defaultPackage = package;
      }
    );
}
