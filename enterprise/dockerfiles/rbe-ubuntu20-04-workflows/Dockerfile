FROM gcr.io/flame-public/rbe-ubuntu20-04@sha256:036ae8c90876fa22da9ace6f8218e614f4cd500a154fc162973fff691e72d28e

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.15.0/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

# Pre-download/extract bazel so that Bazel can skip that work on first run,
# at least for CI runs on the BB repo itself.
RUN USE_BAZEL_VERSION=6.0.0 bazelisk version

# Provision a non-root user named "buildbuddy" and set up passwordless sudo.
# Non-root users are needed for some bazel toolchains, such as hermetic python.
RUN apt-get update && apt-get install -y sudo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home buildbuddy --groups sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
