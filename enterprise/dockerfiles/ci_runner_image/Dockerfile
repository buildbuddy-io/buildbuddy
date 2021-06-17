FROM gcr.io/cloud-marketplace/google/debian10@sha256:c571e553cdaa91b1f16c190a049ccef828234ac47a0e8ef40c84240e62108591

RUN apt-get update && apt-get install -y curl git rpm build-essential

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

# Pre-install bazel 3.7.0 to avoid bazelisk downloading & installing bazel on every
# CI run, at least for CI runs on the BuildBuddy repo itself.
RUN USE_BAZEL_VERSION=3.7.0 bazelisk version
