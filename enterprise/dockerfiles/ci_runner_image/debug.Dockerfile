# syntax=docker/dockerfile:1
FROM gcr.io/cloud-marketplace/google/debian10@sha256:c571e553cdaa91b1f16c190a049ccef828234ac47a0e8ef40c84240e62108591

# Deps needed to build bazel.
RUN apt-get update && apt-get install -y openjdk-11-jdk zip python3 curl git build-essential
RUN ln -s $(which python3) /usr/local/bin/python

# Install bazelisk
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.5/bazelisk-linux-amd64 && \
    chmod +x /usr/local/bin/bazelisk

WORKDIR /root
RUN git clone https://github.com/bduffany/bazel
WORKDIR /root/bazel
# Do a clean build of v4.1.0 first so if we change the debug commit
# below, we don't have to rebuild from scratch.
RUN git checkout 4.1.0
RUN USE_BAZEL_VERSION=4.1.0 bazelisk build //src:bazel-bin

RUN git checkout 7327116cddfe9a4f28687ef902dbb5822ebfa230
RUN USE_BAZEL_VERSION=4.1.0 bazelisk build //src:bazel-bin

RUN cp bazel-bin/src/bazel /usr/local/bin/bazel
RUN git rev-parse HEAD > /root/.bazelcommit

FROM gcr.io/cloud-marketplace/google/debian10@sha256:c571e553cdaa91b1f16c190a049ccef828234ac47a0e8ef40c84240e62108591

RUN apt-get update && apt-get install -y curl git rpm build-essential
COPY --from=0 /usr/local/bin/bazel /usr/local/bin/bazel
COPY --from=0 /root/.bazelcommit /root/.bazelcommit
RUN printf '#!/bin/bash\necho "INFO: bazel built from  https://github.com/bduffany/bazel/tree/$(cat /root/.bazelcommit)"\nbazel "$@"' > /usr/local/bin/bazelisk && \
    chmod +x /usr/local/bin/bazelisk && \
    bazelisk version
