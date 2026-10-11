{
  lib,
  writeShellApplication,
  coreutils,
  freenet,
}:
# The supervised, self-updating node: a thin wrapper that seeds `freenet` into a
# MUTABLE state directory once and then supervises it, so the node can replace
# its own binary the way it does on every other platform. `freenet-node.sh`
# carries the full rationale and the systemd directives it mirrors.
#
# `writeShellApplication` runs shellcheck at build time, so building this
# derivation IS a lint of the script.
writeShellApplication {
  name = "freenet-node";
  runtimeInputs = [ coreutils ];
  text = ''
    # Seed binary for the mutable state directory. Assigned here so the script
    # stays a plain shell file that can be linted and unit-tested on its own
    # (scripts/nix-node-wrapper_test.sh, and the shellcheck step in ci.yml).
    FREENET_NIX_SEED_BINARY="${lib.getExe freenet}"
    ${builtins.readFile ./freenet-node.sh}
  '';
  meta = {
    description = "Supervised, self-updating Freenet node";
    homepage = "https://github.com/freenet/freenet-core";
    license = lib.licenses.agpl3Only;
    mainProgram = "freenet-node";
  };
}
