load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sandboxd_client",
    srcs = ["sandboxd_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandboxd_client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/containers/linux_sandbox/proto:sandboxd_go_proto",
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/nsutil",
        "//server/util/retry",
        "//server/util/status",
        "//server/util/tracing",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//backoff",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
