load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gossip",
    srcs = ["gossip.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/interfaces/gossip",
    visibility = ["//visibility:public"],
    deps = ["@com_github_hashicorp_serf//serf"],
)
