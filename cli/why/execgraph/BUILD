load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execgraph",
    srcs = [
        "graph.go",
        "types.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/why/execgraph",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:spawn_go_proto",
        "@com_github_dominikbraun_graph//:graph",
        "@org_golang_google_protobuf//encoding/protodelim",
    ],
)
