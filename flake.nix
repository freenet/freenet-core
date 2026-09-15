{
  description = "Freenet: a peer-to-peer platform for decentralized applications";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # TWO OUTPUTS, AND THE DIFFERENCE MATTERS. See docs/nix.md.
  #
  #   packages.freenet       a plain source build. Declarative, toolchain-pinned,
  #                          and it NEVER updates itself. For development, and
  #                          for deployments that want `nixos-rebuild` to decide
  #                          which version runs (which must also pass
  #                          `--disable-auto-update`; see docs/nix.md).
  #
  #   packages.freenet-node  the supervised, self-updating node, and the default.
  #                          This is what a peer on the network should run: it
  #                          seeds the binary above into a MUTABLE state
  #                          directory once, then lets the node update itself in
  #                          place, with the same signature verification,
  #                          rollback and crash probation as every other
  #                          platform. `nix/freenet-node.sh` explains why the
  #                          store path cannot host the updater.
  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    let
      inherit (nixpkgs) lib;

      # `rev` exists only for a clean git source, `dirtyRev` only for a dirty
      # one, and NEITHER for a tarball, a `path:` source or an unpacked source
      # drop -- where an unguarded dereference is a hard eval failure rather than
      # a fallback.
      #
      # The no-VCS case yields the EMPTY string, not "unknown": build.rs
      # validates this value as hex and panics otherwise, and empty is its
      # documented "no override" signal. See package.nix.
      gitCommitHash =
        if self ? rev then
          lib.substring 0 12 self.rev
        else if self ? dirtyRev then
          lib.substring 0 12 (lib.removeSuffix "-dirty" self.dirtyRev)
        else
          "";

      # Dirty only when nix positively says so. Treating "no VCS metadata" as
      # dirty would disable auto-update for every source-drop build, which is the
      # one failure this whole design exists to prevent.
      #
      # NOTE the consequence for `path:` sources, which is right for a tarball
      # and surprising for a developer: a `path:` source has neither `rev` nor
      # `dirtyRev`, so this is false, `FREENET_GIT_IS_DIRTY=0` is passed, and
      # that value SUPPRESSES build.rs's own `git` probe -- so `nix build
      # 'path:.'` in a tree with uncommitted changes reports a CLEAN build and
      # the binary auto-updates. `.github/workflows/nix.yml` relies on exactly
      # that (it is how the no-VCS path gets covered at all, since
      # `actions/checkout` always leaves a clean `.git`). Use `nix build .` if
      # you want nix's dirty detection. docs/nix.md says so too.
      gitDirty = self ? dirtyRev;

      sourceDateEpoch = if self ? lastModified then toString self.lastModified else null;
    in
    {
      # NOTE for anyone applying this overlay: `pkgs.freenet` is the BARE
      # binary. Putting it in `environment.systemPackages` gives every user a
      # `freenet` on PATH that never updates itself, and nothing about it says
      # so -- a peer started from it silently falls behind every release. Use
      # `pkgs.freenet-node` for anything that runs a peer; `pkgs.freenet` is for
      # building, packaging and `nix develop`. See docs/nix.md, "The two
      # outputs".
      overlays.default = final: _prev: {
        freenet = final.callPackage ./package.nix {
          inherit gitCommitHash gitDirty sourceDateEpoch;
        };

        freenet-node = final.callPackage ./nix/node.nix { };
      };
    }
    // flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
            self.overlays.default
          ];
        };

        # The repo's pinned toolchain, not whatever nixpkgs-unstable happens to
        # ship: `cargo clippy -- -D warnings` and `cargo fmt` only agree with CI
        # when the compiler version does.
        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };

        # Applied here rather than in the overlay so a downstream consumer of the
        # overlay still gets a plain-nixpkgs package.
        freenet = pkgs.freenet.override { inherit rustPlatform; };
        freenet-node = pkgs.freenet-node.override { inherit freenet; };
      in
      {
        packages = {
          inherit freenet freenet-node;
          default = freenet-node;

          # Retained so `nix run github:freenet/freenet-core#freenet-autoupdate`
          # keeps working for anyone who wired it up before the rename.
          freenet-autoupdate = freenet-node;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ freenet ];
          # Not needed by the freenet binary itself, but release-agent's
          # openssl-sys does not build without them, so `cargo clippy
          # --workspace` inside this shell would fail to compile.
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.openssl ];
          packages = [
            toolchain
            pkgs.cargo-nextest
            pkgs.jq
            pkgs.nixfmt
            pkgs.pre-commit
            pkgs.python3
            pkgs.shellcheck
          ];

          # nixpkgs' stdenv exports SOURCE_DATE_EPOCH=315532800 (1980-01-01), and
          # crates/core/build.rs now honours it for reproducible packaging. Left
          # set, every `cargo build` in this shell would stamp "Build timestamp:
          # 1980-01-01T00:00:00Z" into the binary -- the field ops uses to
          # correlate a running node's log with the artifact it came from.
          shellHook = "unset SOURCE_DATE_EPOCH";
        };
      }
    );
}
