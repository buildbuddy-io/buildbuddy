load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testbazelisk",
    testonly = 1,
    srcs = ["testbazelisk.go"],
    data = ["@com_github_bazelbuild_bazelisk//:bazelisk"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazelisk",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)
