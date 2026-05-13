load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "peerset",
    srcs = ["peerset.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/peerset",
    visibility = ["//visibility:public"],
    deps = ["//server/util/lib/set"],
)

go_test(
    name = "peerset_test",
    size = "small",
    srcs = ["peerset_test.go"],
    deps = [
        ":peerset",
        "//server/util/lib/set",
        "@com_github_stretchr_testify//assert",
    ],
)
