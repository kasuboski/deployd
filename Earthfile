VERSION 0.8

IMPORT github.com/earthly/lib/rust:3.0.1 AS rust

install:
  FROM rust:1.78.0-bookworm
  # RUN apt-get update -qq
  # RUN apt-get install --no-install-recommends -qq autoconf autotools-dev libtool-bin clang cmake bsdmainutils
  RUN rustup component add clippy
  RUN rustup component add rustfmt
  # Call +INIT before copying the source file to avoid installing dependencies every time source code changes. 
  # This parametrization will be used in future calls to functions of the library
  DO rust+INIT --keep_fingerprints=true

source:
  FROM +install
  COPY --keep-ts Cargo.toml Cargo.lock ./
  COPY --keep-ts --dir src ./

lint:
  FROM +source
  DO rust+CARGO --args="clippy --all-features --all-targets -- -D warnings"

build:
  FROM +lint
  DO rust+CARGO --args="build --release" --output="release/[^/\.]+"
  SAVE ARTIFACT ./target/release/* deployd AS LOCAL result/deployd

test:
  FROM +lint
  DO rust+CARGO --args="test"

image:
  ARG EARTHLY_GIT_SHORT_HASH
  ARG TAG=$EARTHLY_GIT_SHORT_HASH
  ARG TARGETARCH
  ARG TARGETOS
  FROM cgr.dev/chainguard/glibc-dynamic
  WORKDIR /deployd
  COPY +build/deployd /app/deployd
  EXPOSE 3030
  CMD ["/app/deployd"]
  SAVE IMAGE --push ghcr.io/kasuboski/deployd:$TAG-$TARGETOS-$TARGETARCH