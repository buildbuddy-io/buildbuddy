FROM gcr.io/cloud-marketplace/google/debian10@sha256:c571e553cdaa91b1f16c190a049ccef828234ac47a0e8ef40c84240e62108591

RUN apt-get update && apt-get install -y curl git rpm build-essential

RUN apt update && apt install -y openjdk-11-jdk
RUN apt update && apt install -y zip
RUN apt update && apt install -y python3

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

# Pre-install bazel to avoid bazelisk downloading & installing bazel on every
# CI run, at least for CI runs on the BuildBuddy repo itself.
RUN USE_BAZEL_VERSION=4.1.0 bazelisk version

WORKDIR /root
RUN git clone https://github.com/bduffany/bazel
WORKDIR /root/bazel

RUN ln -s $(which python3) /usr/local/bin/python

RUN bazelisk build //src:bazel-bin

RUN git checkout 20cdd150ba2d66df8b7433ada7ffa75d1bf22e9d
RUN bazelisk build //src:bazel-bin
RUN cp bazel-bin/src/bazel /usr/local/bin/bazel
RUN printf '#!/bin/bash\necho "INFO: bazel built from  https://github.com/bduffany/bazel/tree/$(cd ~/bazel && git rev-parse HEAD)"\nbazel "$@"' > /usr/local/bin/bazelisk && \
    chmod +x /usr/local/bin/bazelisk
RUN bazelisk version
