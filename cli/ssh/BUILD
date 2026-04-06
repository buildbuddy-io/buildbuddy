load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ssh",
    srcs = ["ssh.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/ssh",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/status",
        "//server/util/wgkeys",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_crypto//ssh",
        "@org_golang_x_crypto//ssh/knownhosts",
        "@org_golang_x_term//:term",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
