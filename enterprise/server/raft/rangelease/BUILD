load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "rangelease",
    srcs = ["rangelease.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/nodelease",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_lni_dragonboat_v3//:dragonboat",
    ],
)
