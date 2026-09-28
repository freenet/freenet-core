# NixOS VM test for `services.freenet-node` (nix/module.nix). Run with
#   nix build .#checks.x86_64-linux.nixos-module -L
#
# The VM has no internet, so this proves the parts the module is responsible
# for — the seed lands in the state directory, the node runs from there rather
# than from the read-only store, and the directories have the right owner and
# mode — not that the peer joins the network or updates itself. Those are
# covered by the wrapper suite (scripts/nix-node-wrapper_test.sh) and the
# auto-update canaries.
{ self, pkgs }:
pkgs.testers.runNixOSTest {
  name = "freenet-nixos-module";

  nodes.machine = {
    imports = [ self.nixosModules.default ];
    services.freenet-node.enable = true;
  };

  testScript = ''
    machine.wait_for_unit("freenet-node.service")

    # Seeded into the writable state directory as a regular file, not a
    # symlink into the store: the in-place updater renames over this path.
    machine.wait_until_succeeds("test -f /var/lib/freenet/bin/freenet", timeout=120)
    machine.succeed("test ! -L /var/lib/freenet/bin/freenet")
    machine.succeed("/var/lib/freenet/bin/freenet --version")

    # The node process runs the seeded binary. Run from the store, every
    # update would fail with EROFS and the peer would never update itself.
    machine.wait_until_succeeds(
        "pgrep -u freenet -f '^/var/lib/freenet/bin/freenet network'", timeout=120
    )
    machine.fail("pgrep -u freenet -f '^/nix/store/.*/bin/freenet network'")

    # Owned by the service user, private (the config directory holds keys),
    # and the user's home is the state directory, so the node's auto-update
    # state is writable.
    machine.succeed("test \"$(stat -c %U:%a /var/lib/freenet)\" = freenet:700")
    machine.succeed("test \"$(getent passwd freenet | cut -d: -f6)\" = /var/lib/freenet")
    machine.succeed("runuser -u freenet -- test -w /var/lib/freenet")
  '';
}
