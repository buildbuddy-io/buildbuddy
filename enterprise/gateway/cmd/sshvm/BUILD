load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sshvm_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/cmd/sshvm",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/wgkeys",
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_creack_pty//:pty",
        "@com_github_gliderlabs_ssh//:ssh",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_crypto//ssh",
    ],
)

go_binary(
    name = "sshvm",
    embed = [":sshvm_lib"],
    visibility = ["//visibility:public"],
)
