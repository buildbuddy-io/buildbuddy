load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def tensorflow_cgo_deps():
    http_archive(
        name = "libtensorflow-cpu-linux-x86_64-${TENSORFLOW_VERSION}",
        sha256 = "${TENSORFLOW_SHA256}",
        urls = [
            "https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-linux-x86_64-${TENSORFLOW_VERSION}.tar.gz",
        ],
        build_file_content = "filegroup(name = \"libtensorflow\", srcs = glob([\"lib/*\"]), visibility = [\"//enterprise:__subpackages__\"])",
    )
