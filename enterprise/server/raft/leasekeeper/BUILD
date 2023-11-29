load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "leasekeeper",
    srcs = ["leasekeeper.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/leasekeeper",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/events",
        "//enterprise/server/raft/listener",
        "//enterprise/server/raft/nodeliveness",
        "//enterprise/server/raft/rangelease",
        "//proto:raft_go_proto",
        "//server/util/log",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_lni_dragonboat_v4//raftio",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
