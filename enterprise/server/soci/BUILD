load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "soci",
    srcs = ["soci.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/soci",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:soci_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
    ],
)
