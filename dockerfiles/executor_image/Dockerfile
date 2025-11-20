# NOTE: this is a multi-platform image, so each step needs to work on both amd64 and arm64.
# See README for details.

# debian 12
FROM mirror.gcr.io/debian@sha256:7ca9335a5953654f8feb8b846ad3dd78309a838d62b090c785d1f5908c2d59d2

RUN \
    # /etc/apt/debian.sources doesn't pin the debian release by name, but by `stable`.
    # Now that debian 13 was upgraded to stable, the `apt-get upgrade` command below
    # will attempt to upgrade the OS to debian 13. Manually pin it to debian 12 (bookworkm).
    sed -i 's/^Suites: stable stable-updates$/Suites: bookworm bookworm-updates/' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/^Suites: stable-security$/Suites: bookworm-security/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y \
    # Necessary for rules_go
    gcc \
    # Necessary for rules_pkg
    python3 \
    # Necessary to build redis, for some reason???
    libstdc++-14-dev \
    # Necessary for firecracker
    e2fsprogs \
    # FUSE support, used by vbd and vfs packages.
    # TODO: probably not needed since we use DirectMountStrict now?
    fuse \
    # Networking tools used by networking package.
    # iproute2 provides the "ip" command.
    iproute2 conntrack \
    # Credential helper for custom execution images hosted on ECR.
    amazon-ecr-credential-helper \
    # cgroup-aware proc mounts for OCI containers.
    lxcfs && \
    apt-get remove -y docker.io docker-cli containerd runc docker-buildx && \
    # Can't safely run autoremove, too many builds have non-hermetic implicit dependencies.
    # TODO(zoey): investigate common implicit deps and install them explicitly
    # so we can run autoremove
    # apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

RUN DOCKER_VERSION="5:28.2.2-1~debian.12~bookworm" && \
    CONTAINERD_DEB_VERSION="1.7.27-1" && \
    DOCKER_BUILDX_VERSION="0.24.0-1~debian.12~bookworm" && \
    DOCKER_COMPOSE_VERSION="2.36.2-1~debian.12~bookworm" && \
    apt-get update && \
    apt-get install -y \
    curl ca-certificates apt-transport-https && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
    chmod a+r /etc/apt/keyrings/docker.asc && \
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y \
    docker-ce=${DOCKER_VERSION} \
    docker-ce-cli=${DOCKER_VERSION} \
    docker-ce-rootless-extras=${DOCKER_VERSION} \
    containerd.io=${CONTAINERD_DEB_VERSION} \
    docker-buildx-plugin=${DOCKER_BUILDX_VERSION} \
    docker-compose-plugin=${DOCKER_COMPOSE_VERSION} && \
    apt-mark auto \
    curl ca-certificates apt-transport-https && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean && \
    rm \
    # keep these in sync with the binaries we are manually
    # adding in enterprise/server/cmd/executor/BUILD.
    #
    # containerd
    /usr/bin/containerd \
    /usr/bin/containerd-shim \
    /usr/bin/containerd-shim-runc-v1 \
    /usr/bin/ctr \
    # rootlesskit
    /usr/bin/rootlesskit \
    # runc
    /usr/bin/runc
