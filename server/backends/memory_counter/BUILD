load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["memory_counter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_counter",
    visibility = ["//visibility:public"],
    deps = ["@com_github_hashicorp_golang_lru//:go_default_library"],
)
