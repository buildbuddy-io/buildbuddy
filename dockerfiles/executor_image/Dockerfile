# Debian 13 (trixie) — multi-arch
FROM mirror.gcr.io/debian@sha256:cc92da07b99dd5c078cb5583fdb4ba639c7c9c14eb78508a2be285ca67cc738a

# Base utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      # FUSE support (Debian 13 uses fuse3; 'fuse' is transitional)
      fuse3 \
      # Networking tools
      iproute2 conntrack \
      # ECR credential helper (in Debian repos)
      amazon-ecr-credential-helper \
      # cgroup-aware /proc for containers
      lxcfs && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

# Docker Engine + containerd/buildx/compose from Docker's APT repo for trixie
RUN DOCKER_VERSION="5:28.3.3-1~debian.13~trixie" && \
    CONTAINERD_DEB_VERSION="1.7.27-1" && \
    DOCKER_BUILDX_VERSION="0.26.1-1~debian.13~trixie" && \
    DOCKER_COMPOSE_VERSION="2.39.1-1~debian.13~trixie" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      curl ca-certificates && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
    chmod a+r /etc/apt/keyrings/docker.asc && \
    sh -lc 'echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list' && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      docker-ce=${DOCKER_VERSION} \
      docker-ce-cli=${DOCKER_VERSION} \
      docker-ce-rootless-extras=${DOCKER_VERSION} \
      containerd.io=${CONTAINERD_DEB_VERSION} \
      docker-buildx-plugin=${DOCKER_BUILDX_VERSION} \
      docker-compose-plugin=${DOCKER_COMPOSE_VERSION} && \
    apt-mark auto curl ca-certificates && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean && \
    rm \
      /usr/bin/containerd \
      /usr/bin/containerd-shim \
      /usr/bin/containerd-shim-runc-v1 \
      /usr/bin/ctr \
      /usr/bin/rootlesskit \
      /usr/bin/runc
