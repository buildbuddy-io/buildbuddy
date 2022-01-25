FROM gcr.io/cloud-marketplace/google/rbe-ubuntu18-04@sha256:48b67b41118dbcdfc265e7335f454fbefa62681ab8d47200971fc7a52fb32054

RUN add-apt-repository ppa:git-core/ppa
RUN apt-get update && apt-get install -y build-essential git

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

# Pre-install bazel 4.1.0 to avoid bazelisk downloading & installing bazel on every
# CI run, at least for CI runs on the BuildBuddy repo itself.
RUN USE_BAZEL_VERSION=4.1.0 bazelisk version
