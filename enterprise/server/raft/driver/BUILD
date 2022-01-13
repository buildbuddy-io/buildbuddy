load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "driver",
    srcs = ["driver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/rangelease",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//raftio",
    ],
)
