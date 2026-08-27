load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "bazelrbe_lib",
    srcs = ["bazelrbe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/bazelrbe",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "bazelrbe_test",
    srcs = ["bazelrbe_test.go"],
    embed = [":bazelrbe_lib"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_binary(
    name = "bazelrbe",
    embed = [":bazelrbe_lib"],
    visibility = ["//visibility:public"],
)
