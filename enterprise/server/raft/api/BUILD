load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "api",
    srcs = ["api.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/api",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/config",
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/util/disk",
        "//server/util/log",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//statemachine",
    ],
)
