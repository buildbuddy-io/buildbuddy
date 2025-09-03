load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "batch_operator",
    srcs = ["batch_operator.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/log",
        "@org_golang_google_grpc//status",
    ],
)
