load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "uffd",
    srcs = ["uffd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/blockio",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
