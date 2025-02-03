FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y \
        ca-certificates \
        conntrack \
        curl \
        dnsutils \
        iproute2 \
        iptables \
        iputils-ping \
        net-tools \
        && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
