# NOTE: this is a multi-platform image, so each step needs to work on both amd64 and arm64.
# See README for details.

# debian 12
FROM debian@sha256:4ad0bf44ff2401602528e3963a177ebeae8aae7296b80dfe361970c55c87b018

RUN apt-get update && \
    apt-get install -y \
		# Install skopeo and umoci which we use to unpack OCI images when we're not using docker.
		# Install iproute2 ("ip" command) to configure networking on host.
    fuse skopeo umoci iproute2 amazon-ecr-credential-helper lxcfs && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

RUN DOCKER_VERSION="5:27.4.1-1~debian.12~bookworm" && \
    CONTAINERD_DEB_VERSION="1.7.24-1" && \
    DOCKER_BUILDX_VERSION="0.19.3-1~debian.12~bookworm" && \
    DOCKER_COMPOSE_VERSION="2.32.1-1~debian.12~bookworm" && \
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
    /usr/bin/rootlesskit-docker-proxy \
    # runc
    /usr/bin/runc
