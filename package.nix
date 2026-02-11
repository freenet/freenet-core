{
  rustPlatform,
  lib,
}:
rustPlatform.buildRustPackage {
  pname = "freenet";
  version = (fromTOML (builtins.readFile ./crates/core/Cargo.toml)).package.version;

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

  meta = with lib; {
    homepage = "https://github.com/freenet/freenet-core";
    license = licenses.agpl3Only;
  };
}
