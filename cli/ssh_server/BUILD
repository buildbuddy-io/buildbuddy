load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ssh_server",
    srcs = ["ssh_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/ssh_server",
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
        "//server/util/uuid",
        "//server/util/wgkeys",
        "@com_github_creack_pty//:pty",
        "@com_github_gliderlabs_ssh//:ssh",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_crypto//ssh",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "ssh_server_test",
    srcs = ["ssh_server_test.go"],
    embed = [":ssh_server"],
    deps = ["@com_github_stretchr_testify//require"],
)
