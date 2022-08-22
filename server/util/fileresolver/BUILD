load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "fileresolver",
    srcs = ["fileresolver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fileresolver",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

go_test(
    name = "fileresolver_test",
    size = "small",
    srcs = ["fileresolver_test.go"],
    deps = [
        ":fileresolver",
        "//server/util/fileresolver/test_data:bundle",
        "@com_github_stretchr_testify//require",
    ],
)
