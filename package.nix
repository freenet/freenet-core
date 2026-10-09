{
  lib,
  rustPlatform,
  # Git provenance for `crates/core/build.rs`.
  #
  # EMPTY means "no override" -- build.rs then runs its own `git` probe, which
  # yields "unknown" when there is no repository (the nix sandbox case). Do NOT
  # default this to the string "unknown": build.rs validates the value as hex and
  # PANICS on anything else, so a placeholder turns "no VCS information" into a
  # failed build. That defect broke `nix build 'path:.'`, a release tarball, and
  # any downstream `pkgs.callPackage ./package.nix { }`.
  gitCommitHash ? "",
  # Whether the source tree was dirty. This gates auto-update
  # (`auto_update_is_disabled`, crates/core/src/bin/freenet.rs), so it must never
  # be guessed: absence of VCS metadata is NOT dirtiness.
  gitDirty ? false,
  # stdenv's default SOURCE_DATE_EPOCH is 1980-01-01 -- reproducible, and useless
  # in a bug report. The flake passes the source's own timestamp, which is both.
  sourceDateEpoch ? null,
}:
assert lib.assertMsg
  (
    gitCommitHash == ""
    || (
      builtins.stringLength gitCommitHash <= 40 && builtins.match "[0-9a-fA-F]+" gitCommitHash != null
    )
  )
  "gitCommitHash (${gitCommitHash}) must be <= 40 hex characters, or empty for 'no VCS information'";
rustPlatform.buildRustPackage {
  pname = "freenet";
  version = (builtins.fromTOML (builtins.readFile ./crates/core/Cargo.toml)).package.version;

  src = ./.;

  cargoLock.lockFile = ./Cargo.lock;

  env = {
    FREENET_GIT_COMMIT_HASH = gitCommitHash;
    # Not a Nix bool: `false` coerces to "", which build.rs reads as "no
    # override" and would fall back to probing a repository that is not there.
    FREENET_GIT_IS_DIRTY = if gitDirty then "1" else "0";
  }
  // lib.optionalAttrs (sourceDateEpoch != null) {
    SOURCE_DATE_EPOCH = sourceDateEpoch;
  };

  cargoBuildFlags = [
    "--package=freenet"
    "--bin=freenet"
  ];

  # The workspace's test suite needs a network simulation harness and several
  # feature combinations; it is CI's job, not the packager's.
  doCheck = false;

  meta = {
    description = "Peer-to-peer platform for decentralized applications";
    homepage = "https://github.com/freenet/freenet-core";
    license = lib.licenses.agpl3Only;
    mainProgram = "freenet";
  };
}
