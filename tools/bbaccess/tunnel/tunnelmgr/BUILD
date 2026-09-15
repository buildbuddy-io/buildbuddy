load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "tunnelmgr",
    srcs = ["tunnelmgr.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelmgr",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/relaywire",
        "//server/util/wgkeys",
        "//tools/bbaccess/tunnel/config",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "tunnelmgr_test",
    srcs = ["tunnelmgr_test.go"],
    embed = [":tunnelmgr"],
    deps = [
        "//server/util/status",
        "//tools/bbaccess/tunnel/config",
        "//tools/bbaccess/tunnel/credentials",
        "@com_github_stretchr_testify//require",
    ],
)
