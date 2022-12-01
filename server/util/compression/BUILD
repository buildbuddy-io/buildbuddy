load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "compression",
    srcs = ["compression.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/compression",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@com_github_klauspost_compress//zstd",
    ],
)

go_test(
    name = "compression_test",
    srcs = ["compression_test.go"],
    embed = [":compression"],
    deps = ["@com_github_stretchr_testify//require"],
)
