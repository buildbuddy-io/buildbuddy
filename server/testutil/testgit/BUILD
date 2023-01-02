load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "testgit",
    testonly = 1,
    srcs = ["testgit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testgit",
    deps = [
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "//server/util/status",
    ],
)
