load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nbdio",
    srcs = ["nbdio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdio",
    visibility = ["//visibility:public"],
    deps = ["//proto:nbd_go_proto"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
