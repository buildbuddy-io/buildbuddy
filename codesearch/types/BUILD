load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "types",
    srcs = ["types.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/types",
    visibility = ["//visibility:public"],
    deps = ["@com_github_cespare_xxhash_v2//:xxhash"],
)
