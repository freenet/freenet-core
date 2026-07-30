{
  lib,
  rustPlatform,
  gitCommitHash ? "unknown",
  gitDirty ? false,
}:
rustPlatform.buildRustPackage {
  pname = "freenet";
  version = (builtins.fromTOML (builtins.readFile ./crates/core/Cargo.toml)).package.version;

  src = ./.;

  cargoLock.lockFile = ./Cargo.lock;

  env = {
    FREENET_GIT_COMMIT_HASH = gitCommitHash;
    FREENET_GIT_IS_DIRTY = gitDirty;
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
