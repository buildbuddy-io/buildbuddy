load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "executor_auth",
    srcs = ["executor_auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth",
    visibility = ["//visibility:public"],
    deps = ["//server/util/flag"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
