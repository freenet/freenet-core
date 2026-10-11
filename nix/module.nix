# NixOS module: `services.freenet-node`, a supervised, self-updating Freenet peer.
#
# This is the unit from docs/nix.md ("Running it under systemd on NixOS")
# packaged as a module, so an operator writes `services.freenet-node.enable = true;`
# instead of copying a unit whose settings look optional and are not. Every
# directive below is explained there; the comments here only mark which ones
# are load-bearing, so nobody "tidies" them away.
#
# Not `services.freenet`: nixpkgs already uses that name as a renamed-option
# alias for `services.hyphanet` (the Java Freenet, now Hyphanet), so setting it
# enables that module instead.
#
# `self` is this flake, so the default package is the flake's own
# `freenet-node`, built with the pinned toolchain. It is deliberately NOT
# `pkgs.freenet-node` from the overlay, which builds with whatever Rust the
# consumer's nixpkgs has.
self:
{
  config,
  lib,
  options,
  pkgs,
  utils,
  ...
}:
let
  cfg = config.services.freenet-node;
  stateDir = "/var/lib/freenet";
  # Whether the Java Freenet (now Hyphanet) is enabled on this host AND runs as
  # the `freenet` user in /var/lib/freenet: always under its old name
  # `services.freenet`, and as `services.hyphanet` until stateVersion 26.11,
  # when nixpkgs moved it to its own `hyphanet` user and directory.
  javaFreenetSharesOurUser =
    if options.services ? hyphanet then
      config.services.hyphanet.enable
      && lib.versionOlder config.system.stateVersion "26.11"
    else
      config.services.freenet.enable or false;
in
{
  options.services.freenet-node = {
    enable = lib.mkEnableOption "a Freenet peer (supervised and self-updating)";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.freenet-node;
      defaultText = lib.literalExpression "freenet.packages.\${pkgs.stdenv.hostPlatform.system}.freenet-node";
      description = ''
        The supervised node to run. It must provide `bin/freenet-node`: the
        bare `freenet` package has no supervisor and never updates itself, so
        a peer started from it silently falls behind every release.
      '';
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [
        "--network-port"
        "31337"
      ];
      description = "Extra arguments passed through to `freenet network`.";
    };
  };

  config = lib.mkIf cfg.enable {
    # Otherwise the two would share a user, a state directory and its logs/
    # without any error.
    assertions = [
      {
        assertion = !javaFreenetSharesOurUser;
        message = "services.freenet-node cannot run alongside the Java Freenet (services.hyphanet / services.freenet) on this host: both use the `freenet` user and /var/lib/freenet. On stateVersion 26.11 or later Hyphanet uses its own user and directory.";
      }
    ];

    users.users.freenet = {
      isSystemUser = true;
      group = "freenet";
      # Load-bearing for any binary older than the auto-update state-dir
      # fallback: without a writable home the node cannot persist its
      # crash-probation marker, rollback snapshot or known-bad pin, and #4073
      # crash-loop rollback is silently off. Pointing it at the state
      # directory keeps everything in one place.
      home = stateDir;
    };
    users.groups.freenet = { };

    # Not named `freenet`: `freenet update` probes and rewrites
    # /etc/systemd/system/freenet.service, which on NixOS is a store symlink.
    systemd.services.freenet-node = {
      description = "Freenet peer";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      # Load-bearing: freenet-node runs its own crash-loop limiter, and a
      # systemd start limit on top of it can park the unit in `failed` forever.
      startLimitIntervalSec = 0;
      serviceConfig = {
        # systemd's own quoting, which also escapes `%` specifiers and `$`
        # in extraArgs; shell escaping leaves both for systemd to expand.
        ExecStart = utils.escapeSystemdExecArgs (
          [
            (lib.getExe' cfg.package "freenet-node")
            "--config-dir"
            "${stateDir}/config"
            "--data-dir"
            "${stateDir}/data"
            "--log-dir"
            "${stateDir}/logs"
          ]
          ++ cfg.extraArgs
        );
        User = "freenet";
        Group = "freenet";
        # The wrapper seeds the self-updating binary into $STATE_DIRECTORY/bin.
        # "freenet/config" is load-bearing: the node refuses an explicit
        # --config-dir that does not exist (a typo must not silently create a
        # fresh identity). data and logs are listed for symmetry; the node would
        # create those itself. $STATE_DIRECTORY becomes a colon-separated list;
        # the wrapper and the node both take its first entry, /var/lib/freenet.
        StateDirectory = [
          "freenet"
          "freenet/config"
          "freenet/data"
          "freenet/logs"
        ];
        # The config directory holds the node's private keys.
        StateDirectoryMode = "0700";
        # Load-bearing: the wrapper exits 0 for a stood-down peer (exit 43,
        # port already held) as well as a clean shutdown, and only `always`
        # brings it back. `systemctl stop` still stops it.
        Restart = "always";
        RestartSec = 30;
        NoNewPrivileges = true;
        PrivateTmp = true;
      };
    };
  };
}
