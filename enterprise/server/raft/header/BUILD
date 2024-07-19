load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "header",
    srcs = ["header.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header",
    visibility = ["//visibility:public"],
    deps = ["//proto:raft_go_proto"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
