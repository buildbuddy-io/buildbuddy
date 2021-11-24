load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "rbuilder",
    srcs = ["rbuilder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:raft_go_proto",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_google_grpc//status",
    ],
)
