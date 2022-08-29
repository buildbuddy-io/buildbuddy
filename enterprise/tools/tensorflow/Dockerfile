# Copyright 2022 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================


# This Dockerfile is derived from
# https://github.com/tensorflow/build/blob/776c02aad97ac75c59306b721bf5a70edad2c9c6/golang_install_guide/example-program/Dockerfile

FROM golang:1.18-bullseye

ARG TENSORFLOW_VERSION

# Install proto deps needed to generate go srcs.
RUN apt-get update && apt-get -y install --no-install-recommends \
    libprotobuf-dev \
    protobuf-compiler

# Clone the TF repo, which includes the Go API package.
# Note: the generation scripts rely on the repo being in GOPATH,
# which is why we clone under /go/src/...
RUN git clone --branch=v${TENSORFLOW_VERSION} https://github.com/tensorflow/tensorflow.git \
   /go/src/github.com/tensorflow/tensorflow

# Install the TensorFlow shared library (required for generating go srcs)
RUN curl -L https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-linux-$(uname -m)-${TENSORFLOW_VERSION}.tar.gz \
    | tee /tensorflow.tar.gz \
    | tar xz --directory /usr/local \
    && ldconfig

# Generate go srcs (mainly protos)
RUN cd /go/src/github.com/tensorflow/tensorflow \
    && go mod init github.com/tensorflow/tensorflow \
    && (cd tensorflow/go/op && go generate) \
    && go mod tidy \
    && go test ./...

# Create a new repo under /tensorflow, with only the go sources.
# We don't need the rest of the TF repo since it includes a lot of deps we don't need.
RUN mkdir -p /tensorflow/tensorflow && \
    cd /go/src/github.com/tensorflow/tensorflow && \
    cp -r tensorflow/go /tensorflow/tensorflow/ && \
    rm /tensorflow/tensorflow/go/BUILD && \
    cp LICENSE .gitignore .bazelversion go.mod go.sum /tensorflow/

WORKDIR /tensorflow
COPY WORKSPACE .

# Install bazel tools.
RUN go install github.com/bazelbuild/bazelisk@latest && \
    ln -s $(which bazelisk) /usr/bin/bazel && \
    bazel version && \
    go install github.com/bazelbuild/buildtools/buildozer@latest && \
    go install github.com/bazelbuild/bazel-gazelle/cmd/gazelle@latest

# Generate BUILD files with gazelle.
# Note: disabling proto target generation since `go generate` already generates protos for us.
RUN gazelle -go_prefix github.com/tensorflow/tensorflow -proto=disable .

# Vendor the TF header files so that the cgo include directives
# can find the headers.
RUN cp -r /usr/local/include/tensorflow/* ./tensorflow/
# Add a BUILD file declaring a cc_library referencing the vendored TF headers
# and lazily downloaded .so (via http_archive - see deps.bzl). Populate
# TF version and TF release sha256 sum.
COPY BUILD.lib.bzl /tmp/BUILD
COPY deps.bzl /tmp/deps.bzl
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && \
    export TENSORFLOW_VERSION=${TENSORFLOW_VERSION} && \
    export TENSORFLOW_SHA256=$(sha256sum /tensorflow.tar.gz | awk '{print $1}') && \
    cat /tmp/BUILD | envsubst > BUILD && \
    cat /tmp/deps.bzl | envsubst > deps.bzl

# Copy the system libgcc and libstdc++ into the repo since TF depends on them.
# We don't want to rely on these being available on the host system -- for
# example, distroless images will not have these available. Together they are
# ~2 MB so copying into the repo should be OK.
RUN apt-get update && apt-get install --no-install-recommends -y libgcc-s1 && \
    cp /lib/x86_64-linux-gnu/libgcc_s.so.1 . && \
    cp /usr/lib/x86_64-linux-gnu/libstdc++.so.6 .

# Make some adjustments to gazelle's default output to link against the TF
# release libraries and headers, and add a "keep" comment to prevent gazelle
# from modifying it.
RUN buildozer 'remove clinkopts' //tensorflow/go && \
    buildozer 'remove copts' //tensorflow/go && \
    buildozer 'add cdeps //:libtensorflow //:libstdc++ //:libgcc_s' //tensorflow/go && \
    buildozer 'comment keep' //tensorflow/go

# Remove the system install of TF to ensure those aren't used in the test build.
RUN rm -rf /usr/local/lib/libtensorflow* /usr/local/include/tensorflow
# Make sure we can build the Go lib.
RUN bazel build //tensorflow/go && rm bazel-*
