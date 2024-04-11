load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "background",
    srcs = ["background.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/background",
    visibility = ["//visibility:public"],
)

go_test(
    name = "background_test",
    srcs = ["background_test.go"],
    deps = [
        ":background",
        "@com_github_stretchr_testify//require",
    ],
)
