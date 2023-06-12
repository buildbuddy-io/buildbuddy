load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "blockio",
    srcs = ["blockio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "blockio_test",
    srcs = ["blockio_test.go"],
    deps = [
        ":blockio",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
