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
        freenet-autoupdate = pkgs.writeShellApplication {
          name = "freenet-autoupdate";
          runtimeInputs = [ pkgs.gh pkgs.nix ];
          text = ''
            while true; do
                vsn="$(gh release view --repo freenet/freenet-core \
                  --json tagName --jq .tagName)"
                echo "Running freenet $vsn"
                exit_code=0
                nix --option experimental-features 'nix-command flakes' \
                  run "github:freenet/freenet-core/$vsn#freenet" || exit_code=$?

                if [ $exit_code -eq 42 ]; then
                    echo "Autoupdate triggered. Restarting..."
                else
                    echo "Non-autoupdate exit code: $exit_code. Stopping."
                    exit $exit_code
                fi
            done
          '';
        };
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
