{
  description = "tractor — a simple, modern, type-safe actor framework";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      uv2nix,
      pyproject-nix,
      pyproject-build-systems,
      ...
    }:
    let
      inherit (nixpkgs) lib;

      # Load the uv workspace from this repo (reads pyproject.toml + uv.lock).
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      # Generated overlay translating the uv lock into Nix derivations.
      # Prefer prebuilt wheels (ruff/pyrefly ship Rust wheels).
      overlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      # Build fixups go here if a package needs extra native inputs.
      pyprojectOverrides = _final: _prev: { };

      forAllSystems = lib.genAttrs lib.systems.flakeExposed;

      pythonSetFor =
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python314;
        in
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (
            lib.composeManyExtensions [
              pyproject-build-systems.overlays.default
              overlay
              pyprojectOverrides
            ]
          );
    in
    {
      # Distribution artifacts for PyPI: sdist (.tar.gz) + wheel (.whl).
      # Built with uv's own backend, which runs fully offline.
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          dist = pkgs.stdenvNoCC.mkDerivation {
            pname = "tractor-dist";
            version = "0.1.0";

            src = lib.fileset.toSource {
              root = ./.;
              fileset = lib.fileset.unions [
                ./pyproject.toml
                ./src
              ];
            };

            nativeBuildInputs = [
              pkgs.uv
              pkgs.python314
            ];

            # No network / cache writes inside the sandbox; use the Nix Python.
            env = {
              UV_OFFLINE = "1";
              UV_NO_CACHE = "1";
              UV_PYTHON = pkgs.python314.interpreter;
              UV_PYTHON_DOWNLOADS = "never";
            };

            buildPhase = ''
              runHook preBuild
              export HOME=$TMPDIR
              uv build --offline --no-cache --out-dir dist
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out
              cp dist/*.tar.gz dist/*.whl $out/
              runHook postInstall
            '';
          };
        in
        {
          default = dist;
          dist = dist;
        }
      );

      # Pure dev shell: uv2nix builds the venv (incl. dev group) from the lock,
      # tractor installed editable so src/ edits are live.
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonSet = pythonSetFor system;

          editableOverlay = workspace.mkEditablePyprojectOverlay {
            root = "$REPO_ROOT";
          };

          editablePythonSet = pythonSet.overrideScope (
            lib.composeManyExtensions [
              editableOverlay
              (final: prev: {
                tractor = prev.tractor.overrideAttrs (old: {
                  src = lib.fileset.toSource {
                    root = old.src;
                    fileset = lib.fileset.unions [
                      (old.src + "/pyproject.toml")
                      (old.src + "/src")
                    ];
                  };
                  nativeBuildInputs =
                    old.nativeBuildInputs
                    ++ final.resolveBuildSystem { editables = [ ]; };
                });
              })
            ]
          );

          virtualenv = editablePythonSet.mkVirtualEnv "tractor-dev-env" workspace.deps.all;
        in
        {
          default = pkgs.mkShell {
            packages = [
              virtualenv
              pkgs.uv
            ];

            env = {
              # Don't let uv touch the Nix-managed environment.
              UV_NO_SYNC = "1";
              UV_PYTHON = "${virtualenv}/bin/python";
              UV_PYTHON_DOWNLOADS = "never";
            };

            shellHook = ''
              unset PYTHONPATH
              export REPO_ROOT=$(git rev-parse --show-toplevel)
            '';
          };
        }
      );
    };
}
