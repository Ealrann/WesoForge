FROM rust:bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        gawk \
        pkg-config \
        libgmp-dev \
        libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

RUN cargo build -p bbr-client --release --features prod-backend

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
        libgmp10 \
        libgmpxx4ldbl \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --home-dir /home/wesoforge --shell /usr/sbin/nologin wesoforge

COPY --from=builder /workspace/target/release/wesoforge /usr/local/bin/wesoforge
COPY docker/entrypoint.sh /usr/local/bin/wesoforge-entrypoint
RUN chmod +x /usr/local/bin/wesoforge-entrypoint

USER wesoforge
WORKDIR /home/wesoforge

ENV BBR_MODE=group
ENV BBR_NO_TUI=true

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/wesoforge-entrypoint"]
CMD ["wesoforge"]
