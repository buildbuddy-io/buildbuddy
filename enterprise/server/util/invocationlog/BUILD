load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "invocationlog",
    srcs = ["invocationlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/invocationlog",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/lockingbuffer",
        "//server/util/redact",
    ],
)

go_test(
    name = "invocationlog_test",
    srcs = ["invocationlog_test.go"],
    embed = [":invocationlog"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
