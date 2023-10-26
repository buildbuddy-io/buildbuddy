load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "events",
    srcs = ["events.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/events",
    visibility = ["//visibility:public"],
    deps = ["//proto:raft_go_proto"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
