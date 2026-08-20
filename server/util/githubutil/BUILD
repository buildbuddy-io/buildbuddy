load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "githubutil",
    srcs = ["githubutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/githubutil",
    deps = [
        "//server/util/flag",
    ],
)
