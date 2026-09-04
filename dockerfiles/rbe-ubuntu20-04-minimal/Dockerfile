FROM docker.io/library/docker:27.0.3-dind AS docker

RUN apk add --no-cache binutils upx && \
    strip --strip-all \
      /usr/local/bin/docker \
      /usr/local/bin/dockerd \
      /usr/local/bin/runc && \
    upx --best --lzma \
      /usr/local/bin/containerd \
      /usr/local/bin/containerd-shim-runc-v2 \
      /usr/local/bin/docker \
      /usr/local/bin/docker-proxy \
      /usr/local/bin/dockerd \
      /usr/local/bin/runc

FROM docker.io/library/ubuntu:20.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates \
      iptables \
      && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=docker \
  /usr/local/bin/containerd \
  /usr/local/bin/containerd-shim-runc-v2 \
  /usr/local/bin/docker \
  /usr/local/bin/docker-proxy \
  /usr/local/bin/dockerd \
  /usr/local/bin/runc \
  /usr/local/bin/

CMD ["bash"]
