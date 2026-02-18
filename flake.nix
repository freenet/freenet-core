{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      ...
    }:
    {
      overlays.default = _: pkgs: {
        freenet = pkgs.callPackage (import ./package.nix) { };
        freenet-autoupdate = pkgs.writeShellScriptBin "freenet-autoupdate" ''
          trap "" INT

          while true; do
              nix run github:freenet/freenet-core#freenet
              exit_code=$?

              if [ $exit_code -eq 42 ]; then
                  echo "Autoupdate triggered. Restarting..."
              else
                  echo "Non-autoupdate exit code: $exit_code. Stopping."
                  break
              fi
          done
        '';
      };
    }
    // flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlays.default ];
        };
      in
      {
        packages = rec {
          inherit (pkgs) freenet freenet-autoupdate;
          default = freenet-autoupdate;
        };
      }
    );
}
