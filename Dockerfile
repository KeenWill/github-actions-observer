FROM rust:1.94.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --locked --release

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/github-actions-observer /usr/local/bin/github-actions-observer
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["github-actions-observer"]
CMD ["serve"]
