FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y ca-certificates iproute2 iptables iputils-ping && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
