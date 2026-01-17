load("@io_bazel_rules_go//go:def.bzl", "go_library")
load(":defs.bzl", "bazel_pkg_tar")

bazel_pkg_tar(
    name = "bazel_binaries_tar",
    versions = [
        "6.5.0",
        "7.7.1",
        "8.3.1",
        "8.4.2",
        "8.5.0",  # used by testbazel
        "9.0.0rc3",
    ],
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
