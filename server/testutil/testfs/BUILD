load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "testfs",
    srcs = ["testfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testfs",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/random",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

go_test(
    name = "testfs_test",
    srcs = ["testfs_test.go"],
    deps = [
        ":testfs",
        "@com_github_stretchr_testify//require",
    ],
)
