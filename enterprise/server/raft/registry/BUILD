load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "registry",
    srcs = ["registry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//server/util/log",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v3//config",
        "@com_github_lni_dragonboat_v3//raftio",
    ],
)
