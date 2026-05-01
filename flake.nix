{
  description = "tack";

  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.xz";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    ...
  }:
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
    );
}
