# NixOS VM test for `services.freenet-node` (nix/module.nix). Run with
#   nix build .#checks.x86_64-linux.nixos-module -L
#
# The VM has no internet, so this proves the parts the module is responsible
# for — the seed lands in the state directory, the node runs from there rather
# than from the read-only store, extraArgs reach `freenet network`, and the
# directories have the right owner and mode — not that the peer joins the
# network or updates itself. Those are covered by the wrapper suite
# (scripts/nix-node-wrapper_test.sh) and the auto-update canaries.
#
# The node runs as an isolated gateway because an ordinary peer with no
# network exits at once ("Cannot initialize node without gateways") and is
# restarted in backoff, so "is it running" would be a race the test could win
# or lose. A gateway with --skip-load-from-network is allowed to start with no
# gateways and stays up. That is node configuration, not module behaviour.
{ self, pkgs }:
pkgs.testers.runNixOSTest {
  name = "freenet-nixos-module";

  nodes.machine = {
    imports = [ self.nixosModules.default ];
    services.freenet-node = {
      enable = true;
      extraArgs = [
        "--is-gateway"
        "--skip-load-from-network"
        "--public-network-address"
        "127.0.0.1"
        "--public-network-port"
        "31337"
        "--network-port"
        "31337"
      ];
    };
  };

  testScript = ''
    machine.wait_for_unit("freenet-node.service")

    # Seeded into the writable state directory as a regular file, not a
    # symlink into the store: the in-place updater renames over this path.
    machine.wait_until_succeeds("test -f /var/lib/freenet/bin/freenet", timeout=120)
    machine.succeed("test ! -L /var/lib/freenet/bin/freenet")
    machine.succeed("/var/lib/freenet/bin/freenet --version")

    # The node process runs the seeded binary, with extraArgs passed through.
    # Run from the store, every update would fail with EROFS and the peer
    # would never update itself.
    node = "pgrep -u freenet -f '^/var/lib/freenet/bin/freenet network .*--is-gateway'"
    machine.wait_until_succeeds(node, timeout=120)
    pid = machine.succeed(node).strip()
    # ...and it STAYS up: the same process a while later, not a crash loop
    # that a single sample happened to catch between restarts.
    machine.sleep(15)
    machine.succeed(f"test \"$({node})\" = {pid}")
    machine.fail("pgrep -u freenet -f '^/nix/store/.*/bin/freenet network'")

    # The directives the module exists to keep: a stood-down wrapper exits 0,
    # so only Restart=always brings it back, and systemd's own start limit must
    # not park the unit in `failed` underneath the wrapper's limiter.
    machine.succeed("test \"$(systemctl show -p Restart --value freenet-node)\" = always")
    machine.succeed(
        "test \"$(systemctl show -p StartLimitIntervalUSec --value freenet-node)\" = 0"
    )
    # ...and it does come back after its main process is killed.
    restarts = int(machine.succeed("systemctl show -p NRestarts --value freenet-node"))
    machine.succeed("systemctl kill --kill-whom=main -s KILL freenet-node")
    machine.wait_until_succeeds(
        f"test \"$(systemctl show -p NRestarts --value freenet-node)\" -gt {restarts}",
        timeout=120,
    )
    machine.wait_for_unit("freenet-node.service")
    machine.wait_until_succeeds(node, timeout=120)

    # Owned by the service user, private (the config directory holds keys),
    # and the user's home is the state directory, so the node's auto-update
    # state is writable.
    machine.succeed("test \"$(stat -c %U:%a /var/lib/freenet)\" = freenet:700")
    machine.succeed("test \"$(getent passwd freenet | cut -d: -f6)\" = /var/lib/freenet")
    machine.succeed("runuser -u freenet -- test -w /var/lib/freenet")
  '';
}
