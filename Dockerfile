FROM rust:1.85-bookworm AS builder

WORKDIR /work
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p allocdb-node --bin allocdb-k8s-layout --bin allocdb-local-cluster

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /work/target/release/allocdb-k8s-layout /usr/local/bin/allocdb-k8s-layout
COPY --from=builder /work/target/release/allocdb-local-cluster /usr/local/bin/allocdb-local-cluster
COPY deploy/kubernetes/entrypoint.sh /usr/local/bin/allocdb-k8s-entrypoint

RUN chmod +x /usr/local/bin/allocdb-k8s-entrypoint \
    && mkdir -p /var/lib/allocdb /run/allocdb \
    && chown -R 65532:65532 /var/lib/allocdb /run/allocdb

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/allocdb-k8s-entrypoint"]
