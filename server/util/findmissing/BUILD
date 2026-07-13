load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "findmissing",
    srcs = ["findmissing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/findmissing",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
    ],
)

go_test(
    name = "findmissing_test",
    srcs = ["findmissing_test.go"],
    deps = [
        ":findmissing",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
