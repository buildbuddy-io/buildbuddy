load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testsaml",
    testonly = 1,
    srcs = ["testsaml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testsaml",
    deps = [
        "//enterprise/server/util/mocksaml",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "@com_github_stretchr_testify//require",
    ],
)
