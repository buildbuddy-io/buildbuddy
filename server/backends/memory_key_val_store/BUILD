load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "memory_key_val_store",
    srcs = ["memory_key_val_store.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_key_val_store",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_hashicorp_golang_lru//:golang-lru",
    ],
)
