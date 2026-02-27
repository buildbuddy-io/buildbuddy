load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "proxy_util",
    srcs = ["proxy_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/authutil",
        "//server/util/bazel_request",
        "//server/util/usageutil",
        "@org_golang_google_grpc//metadata",
    ],
)
