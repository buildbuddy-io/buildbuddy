load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "wgconnect_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/cmd/wgconnect",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/wgkeys",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "wgconnect",
    embed = [":wgconnect_lib"],
    visibility = ["//visibility:public"],
)
