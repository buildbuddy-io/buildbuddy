load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "memory_kvstore",
    srcs = ["memory_kvstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_kvstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_hashicorp_golang_lru//:golang-lru",
    ],
)

go_test(
    name = "memory_kvstore_test",
    srcs = ["memory_kvstore_test.go"],
    deps = [
        ":memory_kvstore",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
