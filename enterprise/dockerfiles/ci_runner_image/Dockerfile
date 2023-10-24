FROM gcr.io/cloud-marketplace/google/rbe-ubuntu18-04@sha256:48b67b41118dbcdfc265e7335f454fbefa62681ab8d47200971fc7a52fb32054

RUN add-apt-repository ppa:git-core/ppa && \
    apt-get update && \
    apt-get install -y build-essential git python3.6-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

# Pre-download/extract bazel so that Bazel can skip that work on first run,
# at least for CI runs on the BB repo itself.
RUN USE_BAZEL_VERSION=5.3.1 bazelisk version

# Docker
#
# Note: gnupg is only needed to install Docker, so we uninstall it at the end of
# this step and also run `apt-get autoremove` to get rid of the unnecessary
# packages it came with.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      lsb-release \
      && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    echo >/etc/apt/sources.list.d/docker.list "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-get update && \
    apt-get install -y \
      docker-ce \
      docker-ce-cli \
      containerd.io \
      && \
    apt-get remove -y gnupg && \
    apt-get autoremove -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Provision a non-root user named "buildbuddy" and set up passwordless sudo.
# Non-root users are needed for some bazel toolchains, such as hermetic python.
RUN apt-get update && apt-get install -y sudo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home buildbuddy --groups sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
