# pipestream-search server + court pipeline examples.
#
# Stage 1 builds the server binary and the court example binaries (the
# pipeline stages); stage 2 is a slim runtime with rclone for pulling the
# seeded corpus from the rustfs object store (deploy/court-e2e).
#
#   docker build -t pipestream-search .
#   docker run --rm -p 50051:50051 pipestream-search \
#     --role=node --index=/shard/shard.tv

FROM rust:1-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --bin pipestream-search \
      --example court_chunks \
      --example court_extract \
      --example court_ingest \
      --example court_query \
      --example court_verify

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fsSL https://downloads.rclone.org/rclone-current-linux-amd64.deb -o /tmp/rclone.deb \
    && dpkg -i /tmp/rclone.deb \
    && rm -f /tmp/rclone.deb \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pipestream-search /usr/local/bin/pipestream-search
COPY --from=build \
    /src/target/release/examples/court_chunks \
    /src/target/release/examples/court_extract \
    /src/target/release/examples/court_ingest \
    /src/target/release/examples/court_query \
    /src/target/release/examples/court_verify \
    /usr/local/bin/
COPY deploy/court-e2e/pipeline/run.sh /deploy/pipeline/run.sh
RUN chmod +x /deploy/pipeline/run.sh
ENTRYPOINT ["pipestream-search"]
