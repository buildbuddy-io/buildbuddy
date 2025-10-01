load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "applecontainer",
    srcs = ["applecontainer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/applecontainer",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
    ],
)
