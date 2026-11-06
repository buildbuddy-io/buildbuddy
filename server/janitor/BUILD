load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "janitor",
    srcs = ["janitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/janitor",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
    ],
)

go_test(
    name = "janitor_test",
    size = "small",
    srcs = ["janitor_test.go"],
    embed = [":janitor"],
    deps = [
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
