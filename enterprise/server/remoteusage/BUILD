load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "remoteusage",
    srcs = ["remoteusage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteusage",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:usage_go_proto",
        "//server/real_environment",
    ],
)
