load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sandboxutil",
    srcs = ["sandboxutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandboxutil",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/containers/linux_sandbox/proto:sandboxd_go_proto",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
