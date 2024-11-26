load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "shlex",
    srcs = ["shlex.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/shlex",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_google_shlex//:shlex",
    ],
)

go_test(
    name = "shlex_test",
    srcs = ["shlex_test.go"],
    deps = [
        ":shlex",
        "@com_github_stretchr_testify//require",
    ],
)
