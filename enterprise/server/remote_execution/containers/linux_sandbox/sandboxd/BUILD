load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "sandboxd_lib",
    srcs = ["sandboxd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandboxd",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/containers/linux_sandbox/proto:sandboxd_go_proto",
        "//enterprise/server/remote_execution/containers/linux_sandbox/sandboxutil",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//status",
    ],
)

go_binary(
    name = "sandboxd",
    embed = [":sandboxd_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
