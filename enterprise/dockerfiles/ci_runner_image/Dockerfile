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

# Provision a non-root user named "buildbuddy".
# Non-root users are needed for some bazel toolchains, such as hermetic python.
RUN useradd --create-home buildbuddy
