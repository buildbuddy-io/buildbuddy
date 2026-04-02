load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "bitset",
    srcs = ["bitset.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bitset",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
    ],
)

go_test(
    name = "bitset_test",
    srcs = ["bitset_test.go"],
    embed = [":bitset"],
    deps = [
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
