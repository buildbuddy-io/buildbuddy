# Adds one layer to an existing image that upgrades only the ca-certificates
# package. Every base layer stays byte-identical. See README.md.
ARG BASE
FROM ${BASE}
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --only-upgrade ca-certificates && \
    rm -rf /var/lib/apt/lists/*
