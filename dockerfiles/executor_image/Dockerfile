# NOTE: this is a multi-platform image, so each step needs to work on both amd64 and arm64.
# See README for details.

# debian 12
FROM mirror.gcr.io/debian@sha256:723a457777ce1b4bebda7c1252131e9779dd775f9b536ed35175a2ba57f2f23f

RUN apt-get update && \
    apt-get install -y \
    # FUSE support, used by vbd and vfs packages.
    # TODO: probably not needed since we use DirectMountStrict now?
    fuse \
    # OCI image tools used by ociconv package.
    # TODO: remove umoci; no longer used.
    skopeo umoci \
    # Networking tools used by networking package.
    # iproute2 provides the "ip" command.
    iproute2 conntrack \
    # Credential helper for custom execution images hosted on ECR.
    amazon-ecr-credential-helper \
    # cgroup-aware proc mounts for OCI containers.
    lxcfs && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

RUN DOCKER_VERSION="5:28.0.4-1~debian.12~bookworm" && \
    CONTAINERD_DEB_VERSION="1.7.27-1" && \
    DOCKER_BUILDX_VERSION="0.22.0-1~debian.12~bookworm" && \
    DOCKER_COMPOSE_VERSION="2.34.0-1~debian.12~bookworm" && \
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
    # Not currently updating these binaries manually, so leave them in.
    # rootlesskit
    # /usr/bin/rootlesskit \
    # runc
    # /usr/bin/runc
