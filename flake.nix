{
  description = "tack";

  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.xz";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    ...
  }:
  # Per-system outputs (packages, apps, devShells). nixosModules is
  # system-independent and is merged in below.
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};

        # The tack binary itself. We use buildGoModule because the module is
        # a single `main` package at the repo root with internal subpackages.
        # CGO is required for github.com/mattn/go-sqlite3.
        tack = pkgs.buildGoModule {
          pname = "tack";
          version = "0.1.0";
          src = ./.;

          # vendorHash pins the Go module download FOD. Update this whenever
          # go.mod / go.sum changes by replacing it with lib.fakeHash and
          # rebuilding to surface the new hash.
          vendorHash = "sha256-2G9Bhflpw0BDcytKB4oOGIi3HR2fFd1rwZFcoIC09TQ=";

          # The repo ships a stale `tack` binary at the root that gets caught
          # by `go test ./...`-style discovery; ignore it during the build.
          subPackages = ["."];

          # mattn/go-sqlite3 needs cgo + a C compiler.
          env.CGO_ENABLED = "1";
        };
      in {
        # `nix fmt` formats the tree with alejandra. CI pins this same
        # binary via `nix run nixpkgs#alejandra` for the format check.
        formatter = pkgs.alejandra;

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.go
          ];
        };

        packages = {
          default = tack;
          tack = tack;
        };

        apps = {
          default = {
            type = "app";
            program = "${tack}/bin/tack";
          };
          tack = {
            type = "app";
            program = "${tack}/bin/tack";
          };
        };
      }
    )
    // {
      # NixOS module. Operators add `imports = [ tack.nixosModules.default ];`
      # and configure via `services.tangled.tack`. Mirrors the upstream
      # Tangled module shape (services.tangled.spindle, .knot, …) so the
      # config namespace is consistent.
      nixosModules.default = {
        lib,
        pkgs,
        ...
      }: {
        imports = [./nix/modules/tack.nix];

        services.tangled.tack.package =
          lib.mkDefault
          self.packages.${pkgs.stdenv.hostPlatform.system}.tack;
      };
      nixosModules.tack = self.nixosModules.default;
    };
}
