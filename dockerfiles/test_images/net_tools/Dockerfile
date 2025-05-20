FROM marketplace.gcr.io/google/ubuntu2204@sha256:b70be2d80535d3fe99e2bc484133da588728add8fc15956d124c478b42e61f2c

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
