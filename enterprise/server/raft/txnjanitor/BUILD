load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "txnjanitor",
    srcs = ["txnjanitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txnjanitor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
