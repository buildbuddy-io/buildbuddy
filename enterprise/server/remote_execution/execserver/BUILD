load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execserver_lib",
    srcs = ["execserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execserver",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/vmexec",
        "//proto:vmexec_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "execserver",
    embed = [":execserver_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
