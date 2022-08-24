# NOTE: This WORKSPACE file is used to allow running test builds of
# the generated Tensorflow bazel repo. It is not actually needed
# in practice.

workspace(name = "com_github_tensorflow_tensorflow_tensorflow_go")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Go

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "f2dcd210c7095febe54b804bb1cd3a58fe8435a909db2ec04e31542631cf715c",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.31.0/rules_go-v0.31.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.31.0/rules_go-v0.31.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "5982e5463f171da99e3bdaeff8c0f48283a7a5f396ec5282910b9e8a49c0dd7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_download_sdk(
    name = "go_sdk_linux",
    goarch = "amd64",
    goos = "linux",
    version = "1.18.1",  # Keep in sync with .github/workflows/checkstyle.yaml
)

go_register_toolchains()

http_archive(
    name = "com_google_protobuf",
    sha256 = "7c9731ff49ebe1cc4a0650a21d40acc099043f4d584b24632bafc0f5328bc3ff",
    strip_prefix = "protobuf-3.19.0",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.19.0/protobuf-all-3.19.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load(":deps.bzl", "tensorflow_cgo_deps")

tensorflow_cgo_deps()
