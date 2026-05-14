load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "seq",
    srcs = ["seq.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/lib/seq",
    visibility = ["//visibility:public"],
)

go_test(
    name = "seq_test",
    size = "small",
    srcs = ["seq_test.go"],
    deps = [
        ":seq",
        "@com_github_stretchr_testify//assert",
    ],
)
