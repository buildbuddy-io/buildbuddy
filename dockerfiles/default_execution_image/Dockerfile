FROM gcr.io/cloud-marketplace/google/rbe-ubuntu16-04@sha256:5464e3e83dc656fc6e4eae6a01f5c2645f1f7e95854b3802b85e86484132d90e

RUN apt-get update && apt-get install -y rpm build-essential
RUN curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.7.4/bazelisk-linux-amd64 && chmod +x /usr/local/bin/bazelisk && /usr/local/bin/bazelisk
