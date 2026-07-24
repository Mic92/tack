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

      repoDir = mkOption {
        type = types.path;
        default = "/var/lib/tack/repos";
        description = ''
          Directory for Tack's sparse workflow-definition checkouts
          (TACK_REPO_DIR). This must be persistent across restarts to avoid
          cloning every repository again; the default lives in StateDirectory.
        '';
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

      # Tekton uses the pod's in-cluster credentials, so there is no token to
      # place in environmentFile. Enabling it outside Kubernetes will make Tack
      # fail fast while constructing the provider.
      tekton = {
        enable = mkOption {
          type = types.bool;
          default = false;
          description = "Enable the in-cluster Tekton provider (TACK_TEKTON_ENABLED).";
        };

        namespace = mkOption {
          type = types.str;
          default = "default";
          description = ''
            Namespace where Tack creates PipelineRuns
            (TACK_TEKTON_NAMESPACE).
          '';
        };
      };

      # The sourcehut token is sensitive and remains in environmentFile. Only
      # the optional public instance URL belongs in the declarative module.
      sourcehut = {
        instance = mkOption {
          type = types.nullOr types.str;
          default = null;
          example = "https://builds.sr.ht";
          description = ''
            Base URL of the builds.sr.ht-compatible service
            (TACK_SOURCEHUT_INSTANCE). The public builds.sr.ht service is used
            when unset.
          '';
        };
      };

      environmentFile = mkOption {
        type = with types; nullOr path;
        default = null;
        example = "/etc/tack.env";
        description = ''
          Additional environment file as defined in {manpage}`systemd.exec(5)`.

          Sensitive values such as {env}`TACK_BUILDKITE_TOKEN`,
          {env}`TACK_BUILDKITE_WEBHOOK_SECRET`, and
          {env}`TACK_SOURCEHUT_TOKEN` belong here so they don't get baked into
          the world-readable Nix store.
        '';
      };

      # Escape hatch for arbitrary systemd `[Service]` settings (e.g.
      # `MemoryMax`, `CPUQuota`, additional sandboxing knobs). These
      # are merged into `serviceConfig` and override the defaults
      # below on conflict, so callers can both add new settings and
      # tweak the ones we set out of the box.
      extraServiceConfig = mkOption {
        type = types.attrsOf types.unspecified;
        default = {};
        example = literalExpression ''
          {
            MemoryMax = "512M";
            CPUQuota = "50%";
          }
        '';
        description = ''
          Extra settings merged into the systemd service's
          `[Service]` section. See {manpage}`systemd.exec(5)` and
          {manpage}`systemd.resource-control(5)` for available
          options. Values here take precedence over the module's
          defaults.
        '';
      };
    };

    config = mkIf cfg.enable {
      systemd.services.tack = {
        description = "Tack Tangled spindle";
        after = ["network.target"];
        wantedBy = ["multi-user.target"];

        # `extraServiceConfig` is merged in last so user-supplied
        # settings override our defaults on conflict.
        serviceConfig =
          {
            # StateDirectory creates /var/lib/tack with the right ownership;
            # both the default dbPath and repoDir live beneath it. Restrictive
            # modes protect provider identifiers and checked-out workflow YAML.
            StateDirectory = "tack";
            StateDirectoryMode = "0700";
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
                "TACK_REPO_DIR=${cfg.repoDir}"
                "TACK_JETSTREAM_URL=${cfg.jetstreamUrl}"
                "TACK_BUILDKITE_WEBHOOK_MODE=${cfg.buildkite.webhookMode}"
              ]
              ++ optional cfg.dev "TACK_DEV=1"
              ++ optional (cfg.buildkite.org != null)
              "TACK_BUILDKITE_ORG=${cfg.buildkite.org}"
              ++ optional cfg.tekton.enable "TACK_TEKTON_ENABLED=1"
              ++ optional cfg.tekton.enable
              "TACK_TEKTON_NAMESPACE=${cfg.tekton.namespace}"
              ++ optional (cfg.sourcehut.instance != null)
              "TACK_SOURCEHUT_INSTANCE=${cfg.sourcehut.instance}";

            ExecStart = "${cfg.package}/bin/tack -addr ${cfg.listenAddr}";
            Restart = "always";

            # Light hardening. Tack only needs network access plus its
            # state directory, so we lock the rest down.
            DynamicUser = true;
            UMask = "0077";
            NoNewPrivileges = true;
            ProtectSystem = "strict";
            ProtectHome = true;
            # StateDirectory is made writable automatically. These exceptions
            # also make custom dbPath/repoDir parents usable under
            # ProtectSystem=strict; operators remain responsible for creating
            # and assigning ownership of custom directories.
            ReadWritePaths = [
              "-${builtins.dirOf (toString cfg.dbPath)}"
              "-${builtins.dirOf (toString cfg.repoDir)}"
            ];
            PrivateTmp = true;
            PrivateDevices = true;
            ProtectKernelTunables = true;
            ProtectKernelModules = true;
            ProtectControlGroups = true;
            RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_UNIX"];
            RestrictNamespaces = true;
            LockPersonality = true;
            MemoryDenyWriteExecute = true;
          }
          // cfg.extraServiceConfig;
      };
    };
  }
