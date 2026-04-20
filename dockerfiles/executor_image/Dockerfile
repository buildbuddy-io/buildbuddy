# NOTE: this is a multi-platform image, so each step needs to work on both amd64 and arm64.
# See README for details.

# Extract nvidia-cdi-hook in an intermediate stage so the final image gets only
# the binary and not any apt/dpkg metadata churn from package installation.
FROM mirror.gcr.io/debian@sha256:01a723bf5bfb21b9dda0c9a33e0538106e4d02cce8f557e118dd61259553d598 AS nvidia_cdi_hook_extractor

RUN NVIDIA_CONTAINER_TOOLKIT_BASE_VERSION="1.19.0-1" && \
    apt-get update && \
    apt-get install -y ca-certificates curl && \
    arch="$(dpkg --print-architecture)" && \
    curl -fsSL \
    "https://nvidia.github.io/libnvidia-container/stable/deb/${arch}/nvidia-container-toolkit-base_${NVIDIA_CONTAINER_TOOLKIT_BASE_VERSION}_${arch}.deb" \
    -o /tmp/nvidia-container-toolkit-base.deb && \
    dpkg-deb -x /tmp/nvidia-container-toolkit-base.deb /tmp/rootfs && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

# debian 12
FROM mirror.gcr.io/debian@sha256:01a723bf5bfb21b9dda0c9a33e0538106e4d02cce8f557e118dd61259553d598

RUN \
    # /etc/apt/debian.sources doesn't pin the debian release by name, but by `stable`.
    sed -i 's/^Suites: stable stable-updates$/Suites: trixie trixie-updates/' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/^Suites: stable-security$/Suites: trixie-security/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y \
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

RUN DOCKER_VERSION="5:29.0.1-1~debian.13~trixie" && \
    CONTAINERD_DEB_VERSION="2.1.5-1~debian.13~trixie" && \
    DOCKER_BUILDX_VERSION="0.30.0-1~debian.13~trixie" && \
    DOCKER_COMPOSE_VERSION="2.40.3-1~debian.13~trixie" && \
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
    /usr/bin/containerd-shim-runc-v2 \
    /usr/bin/ctr \
    # rootlesskit
    # /usr/bin/rootlesskit \
    # runc
    /usr/bin/runc

# nvidia-cdi-hook is needed for the executor to spawn child containers that need
# to access the GPU. CDI takes care of mounting device files and libraries
# needed for the GPU, but it doesn't mount the supporting files used to set up
# the container itself. So, these container support files need to be provisioned
# manually.
#
# For NVIDIA, the relevant container support files are nvidia-cdi-hook as well
# as some configs under /usr/share/vulkan. It's fine to mount /usr/share/vulkan
# from the host since it just contains some JSON manifests, but we can't mount
# nvidia-cdi-hook since it's usually dynamically linked against GLIBC, and we
# can't assume the host GLIBC version will be compatible with the one installed
# in the executor image. Copy only nvidia-cdi-hook from an extractor stage.
COPY --from=nvidia_cdi_hook_extractor /tmp/rootfs/usr/bin/nvidia-cdi-hook /usr/bin/nvidia-cdi-hook
