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
          runtimeInputs = [
            pkgs.curl
            pkgs.nix
          ];
          text = ''
            # So the node exits 42 for us instead of logging that it will not update.
            export FREENET_SUPERVISED=1

            # Mirrors MAX_UPDATE_FAILURES (crates/core/src/bin/commands/update.rs):
            # stop if the node keeps asking to update but the tag never moves.
            max_failures=3
            failures=0
            last_vsn=""

            # ±20% jitter, mirroring UPDATE_REPOLL_JITTER_FRACTION
            # (crates/core/src/bin/commands/auto_update.rs). Every instance
            # polls the same releases/latest redirect and is handed exit 42 at
            # roughly the same wall-clock moment when a release lands, so a
            # fixed sleep makes them all retry in lockstep.
            jittered() {
                printf '%s' "$(( $1 * 8 / 10 + RANDOM % ($1 * 4 / 10 + 1) ))"
            }

            while true; do
                # The auth-free redirect the node itself uses
                # (GITHUB_LATEST_REDIRECT_URL in .../commands/auto_update.rs).
                # Not api.github.com: that spends REST quota shared per source
                # IP (#5102), and needs a token this wrapper should not require.
                vsn="$(curl -fsSL -o /dev/null -w '%{url_effective}' \
                  -A freenet-updater \
                  https://github.com/freenet/freenet-core/releases/latest \
                  | sed -n 's|.*/releases/tag/||p')" || vsn=""

                if [ -z "$vsn" ]; then
                    # A network blip must not kill the supervisor.
                    delay="$(jittered 60)"
                    echo "Could not resolve the latest freenet release; retrying in $delay seconds." >&2
                    sleep "$delay"
                    continue
                fi

                echo "Running freenet $vsn"
                exit_code=0
                nix --extra-experimental-features 'nix-command flakes' \
                  run "github:freenet/freenet-core/$vsn#freenet" || exit_code=$?

                if [ "$exit_code" -eq 42 ]; then
                    if [ "$vsn" = "$last_vsn" ]; then
                        failures=$((failures + 1))
                    else
                        failures=0
                    fi
                    last_vsn="$vsn"

                    if [ "$failures" -ge "$max_failures" ]; then
                        echo "freenet $vsn requested an update $max_failures times without the" >&2
                        echo "released version changing. Giving up." >&2
                        exit 1
                    fi

                    delay="$(jittered $((60 * (failures + 1))))"
                    echo "Autoupdate triggered. Restarting in $delay seconds..."
                    sleep "$delay"
                else
                    echo "Non-autoupdate exit code: $exit_code. Stopping."
                    exit "$exit_code"
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
