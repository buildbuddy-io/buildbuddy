load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filecacheutil",
    srcs = ["filecacheutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/filecacheutil",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
    ],
)
