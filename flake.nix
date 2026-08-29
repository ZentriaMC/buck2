# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under both the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree and the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree.
{
  description = "A flake for hacking on and building buck2";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };

    rust-version = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    my-rust-bin = rust-version.override {
      extensions = [ "rust-analyzer" "rust-src" ];
    };

    in {
      # Zentria: a from-source package, so the fork can be pinned like nixpkgs' prebuilt buck2.
      packages.buck2 = let
        rustPlatform = pkgs.makeRustPlatform { rustc = rust-version; cargo = rust-version; };
      in
        rustPlatform.buildRustPackage {
          pname = "buck2";
          version = "2026-10-28-zentria";

          src = pkgs.lib.cleanSource ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # darwin-libproc, perf-event, termwiz are git dependencies.
            allowBuiltinFetchGit = true;
            outputHashes = {
              "darwin-libproc-0.2.0" = "sha256-acLQy6I23g/bcObbBSpX/fWUQ+iVCAGSQVhiXyTZ8E4=";
              "tonic-0.14.6" = "sha256-0l1N7d+Rzae9Tex1AhpiN2YBqfa52uMQfftOETJQDMI=";
              "zbus-5.14.0" = "sha256-9NCjcsY7AuGHQ9x4/UJUFTjIMUqVWJnHLY83dKmwhLc=";
            };
          };

          cargoBuildFlags = [ "--bin" "buck2" ];
          doCheck = false;

          nativeBuildInputs = [ pkgs.protobuf ];
          # buck2_protoc_dev honours these instead of the vendored protoc, which does not run in the sandbox.
          BUCK2_BUILD_PROTOC = "${pkgs.protobuf}/bin/protoc";
          BUCK2_BUILD_PROTOC_INCLUDE = "${pkgs.protobuf}/include";

          buildInputs = [ pkgs.zstd ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];

          meta = {
            description = "Fast, hermetic, multi-language build system (Zentria fork with the steno event sink)";
            mainProgram = "buck2";
          };
        };
      packages.default = self.packages.${system}.buck2;

      devShells.default = pkgs.mkShell {
        buildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux ([
          pkgs.mold
        ]) ++ [
          # NOTE (aseipp): needed on aarch64-linux, so that the linker can
          # properly find libatomic.so, but harmless elsewhere
          pkgs.stdenv.cc.cc
        ];
        packages = [ my-rust-bin pkgs.dotslash pkgs.python3 pkgs.lld_20 pkgs.clang_20 pkgs.yarn ];
        shellHook =
          ''
            export BUCK2_BUILD_PROTOC=${pkgs.protobuf}/bin/protoc
            export BUCK2_BUILD_PROTOC_INCLUDE=${pkgs.protobuf}/include
          ''
          # enable mold for linux users, for more tolerable link times
          # we have to specify tokio_unstable in the RUSTFLAGS here since they override
          # .cargo/config.toml that is the reasonable place to specify it
          + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            export RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold --cfg=tokio_unstable $RUSTFLAGS"
          '';
      };
    });
}
