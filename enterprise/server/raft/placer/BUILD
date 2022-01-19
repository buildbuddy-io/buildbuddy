load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "placer",
    srcs = ["placer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/placer",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rangelease",
        "//enterprise/server/raft/rbuilder",
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
    ],
)
