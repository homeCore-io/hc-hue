# CI recipes

check: fmt clippy test

fmt:
    cargo fmt -- --check

clippy:
    cargo clippy --all-targets -- \
        -D clippy::correctness \
        -D clippy::suspicious \
        -A clippy::type_complexity \
        -A clippy::too_many_arguments

test:
    cargo test

build:
    cargo build

build-release:
    cargo build --release
