load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "statemachine",
    srcs = ["statemachine.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/statemachine",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:raft_go_proto",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_lni_dragonboat_v3//statemachine",
    ],
)
