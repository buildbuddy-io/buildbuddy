load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flightrecorder",
    srcs = ["flightrecorder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flightrecorder",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "flightrecorder_test",
    size = "small",
    srcs = ["flightrecorder_test.go"],
    embed = [":flightrecorder"],
    deps = ["@com_github_stretchr_testify//require"],
)
