load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "byte_stream_server_proxy",
    srcs = ["byte_stream_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
