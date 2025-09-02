load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "routing",
    srcs = ["routing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/proxy_util",
        "//proto:remote_execution_go_proto",
        "//proto:routing_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
