load("@io_bazel_rules_go//go:def.bzl", "go_library")
load(":defs.bzl", "bazel_pkg_tar")
load(":versions.bzl", "BAZEL_PROBER_VERSIONS")

bazel_pkg_tar(
    name = "bazel_binaries_tar",
    versions = BAZEL_PROBER_VERSIONS,
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazel",
    srcs = ["bazel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)
