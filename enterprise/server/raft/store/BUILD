load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "store",
    srcs = ["store.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/config",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/nodelease",
        "//enterprise/server/raft/rangelease",
        "//enterprise/server/raft/replica",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//raftio",
        "@com_github_lni_dragonboat_v3//statemachine",
    ],
)
