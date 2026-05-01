# NixOS module for the tack spindle. 
{
  config,
  lib,
  ...
}: let
  cfg = config.services.tangled.tack;
in
  with lib; {
    options.services.tangled.tack = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable the tack Tangled spindle.";
      };

      package = mkOption {
        type = types.package;
        description = "Package providing the `tack` binary.";
      };

      listenAddr = mkOption {
        type = types.str;
        default = ":8080";
        description = "HTTP listen address (TACK_LISTEN_ADDR).";
      };

      hostname = mkOption {
        type = types.str;
        example = "tack.example.com";
        description = ''
          Public hostname this spindle is registered under in Tangled
          (matches `sh.tangled.repo.spindle`). Required.
        '';
      };

      ownerDid = mkOption {
        type = types.str;
        example = "did:plc:qfpnj4og54vl56wngdriaxug";
        description = "DID of the spindle operator (TACK_OWNER_DID). Required.";
      };

      dbPath = mkOption {
        type = types.path;
        default = "/var/lib/tack/tack.db";
        description = "Path to the local SQLite store (TACK_DB_PATH).";
      };

      jetstreamUrl = mkOption {
        type = types.str;
        default = "wss://jetstream1.us-west.bsky.network/subscribe";
        description = "Tangled Jetstream WebSocket URL (TACK_JETSTREAM_URL).";
      };

      dev = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Use `ws://` instead of `wss://` for knot event-streams
          (TACK_DEV). Useful when running against a local knot.
        '';
      };

      # Buildkite provider. Token + webhook secret are sensitive and
      # should be supplied via `environmentFile` so they don't end up
      # world-readable in the Nix store.
      buildkite = {
        org = mkOption {
          type = types.nullOr types.str;
          default = null;
          example = "my-org";
          description = ''
            Default Buildkite org for workflows that don't specify one
            (TACK_BUILDKITE_ORG). Required when the Buildkite token is
            supplied via `environmentFile`.
          '';
        };

        webhookMode = mkOption {
          type = types.enum ["token" "signature"];
          default = "token";
          description = ''
            How tack authenticates incoming Buildkite webhooks
            (TACK_BUILDKITE_WEBHOOK_MODE).
          '';
        };
      };

      environmentFile = mkOption {
        type = with types; nullOr path;
        default = null;
        example = "/etc/tack.env";
        description = ''
          Additional environment file as defined in {manpage}`systemd.exec(5)`.

          Sensitive values such as {env}`TACK_BUILDKITE_TOKEN` and
          {env}`TACK_BUILDKITE_WEBHOOK_SECRET` belong here so they
          don't get baked into the world-readable Nix store.
        '';
      };
    };

    config = mkIf cfg.enable {
      systemd.services.tack = {
        description = "Tack Tangled spindle";
        after = ["network.target"];
        wantedBy = ["multi-user.target"];

        serviceConfig = {
          # StateDirectory creates /var/lib/tack with the right
          # ownership; the default dbPath lives there.
          StateDirectory = "tack";
          LogsDirectory = "tack";
          EnvironmentFile = mkIf (cfg.environmentFile != null) cfg.environmentFile;

          # Only the non-secret env vars go inline here. Anything
          # sensitive (token, webhook secret) must come from
          # `environmentFile` to stay out of the Nix store.
          Environment =
            [
              "TACK_LISTEN_ADDR=${cfg.listenAddr}"
              "TACK_HOSTNAME=${cfg.hostname}"
              "TACK_OWNER_DID=${cfg.ownerDid}"
              "TACK_DB_PATH=${cfg.dbPath}"
              "TACK_JETSTREAM_URL=${cfg.jetstreamUrl}"
              "TACK_BUILDKITE_WEBHOOK_MODE=${cfg.buildkite.webhookMode}"
            ]
            ++ optional cfg.dev "TACK_DEV=1"
            ++ optional (cfg.buildkite.org != null)
              "TACK_BUILDKITE_ORG=${cfg.buildkite.org}";

          ExecStart = "${cfg.package}/bin/tack -addr ${cfg.listenAddr}";
          Restart = "always";

          # Light hardening. Tack only needs network access plus its
          # state directory, so we lock the rest down.
          DynamicUser = true;
          NoNewPrivileges = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
          PrivateDevices = true;
          ProtectKernelTunables = true;
          ProtectKernelModules = true;
          ProtectControlGroups = true;
          RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_UNIX"];
          RestrictNamespaces = true;
          LockPersonality = true;
          MemoryDenyWriteExecute = true;
        };
      };
    };
  }
