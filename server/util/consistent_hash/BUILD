load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "consistent_hash",
    srcs = ["consistent_hash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "consistent_hash_test",
    size = "small",
    srcs = ["consistent_hash_test.go"],
    deps = [
        ":consistent_hash",
        "//server/util/log",
        "//server/util/random",
        "@com_github_stretchr_testify//assert",
    ],
)
