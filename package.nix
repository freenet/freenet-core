{
  lib,
  rustPlatform,
  gitCommitHash ? "unknown",
  gitDirty ? false,
  # stdenv's default is 1980-01-01: reproducible, but useless in a bug report.
  # The flake passes `self.lastModified`, which is both.
  sourceDateEpoch ? null,
}:
assert lib.assertMsg (
  gitCommitHash == "unknown"
  || (
    builtins.stringLength gitCommitHash <= 40 && builtins.match "[0-9a-fA-F]+" gitCommitHash != null
  )
) "gitCommitHash (${gitCommitHash}) must be <= 40 hex characters";
rustPlatform.buildRustPackage {
  pname = "freenet";
  version = (builtins.fromTOML (builtins.readFile ./crates/core/Cargo.toml)).package.version;

  src = ./.;

  cargoLock.lockFile = ./Cargo.lock;

  env = {
    FREENET_GIT_COMMIT_HASH = gitCommitHash;
    # Not a Nix bool: `false` coerces to "", which build.rs reads as "no override".
    FREENET_GIT_IS_DIRTY = if gitDirty then "1" else "0";
  }
  // lib.optionalAttrs (sourceDateEpoch != null) {
    SOURCE_DATE_EPOCH = sourceDateEpoch;
  };

  cargoBuildFlags = [
    "--package=freenet"
    "--bin=freenet"
  ];

  doCheck = false;

  meta = {
    description = "Peer-to-peer platform for decentralized applications";
    homepage = "https://github.com/freenet/freenet-core";
    donationPage = "https://freenet.org/donate/";
    license = lib.licenses.agpl3Only;
    mainProgram = "freenet";
  };
}
