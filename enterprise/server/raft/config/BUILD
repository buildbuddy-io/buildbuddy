load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config",
    visibility = ["//visibility:public"],
    deps = ["@com_github_lni_dragonboat_v3//config"],
)
