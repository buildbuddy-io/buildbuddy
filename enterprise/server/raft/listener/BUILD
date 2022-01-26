load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "listener",
    srcs = ["listener.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@com_github_lni_dragonboat_v3//raftio",
    ],
)
