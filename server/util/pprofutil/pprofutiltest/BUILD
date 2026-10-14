load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "pprofutiltest",
    testonly = 1,
    srcs = ["pprofutiltest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/pprofutil/pprofutiltest",
    visibility = ["//visibility:public"],
    deps = ["@com_github_stretchr_testify//require"],
)
