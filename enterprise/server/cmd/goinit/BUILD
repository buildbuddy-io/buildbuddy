load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(srcs = ["main.go"])

go_library(
    name = "goinit_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/goinit",
    deps = [
        "//enterprise/server/util/vsock",
        "//enterprise/server/vmexec",
        "//enterprise/server/vmvfs:vmvfs_lib",
        "//proto:health_go_proto",
        "//proto:vmexec_go_proto",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/rlimit",
        "//server/util/status",
        "@com_github_jsimonetti_rtnetlink//rtnl",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "goinit",
    embed = [":goinit_lib"],
    pure = "on",
    static = "on",
)
