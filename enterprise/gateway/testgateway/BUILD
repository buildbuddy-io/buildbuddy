load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testgateway",
    testonly = 1,
    srcs = ["testgateway.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/testgateway",
    deps = [
        "//enterprise/gateway/server",
        "//proto:gateway_go_proto",
        "//proto:gateway_service_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "//server/util/wgkeys",
        "@com_github_stretchr_testify//require",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun/netstack",
        "@org_golang_google_grpc//:grpc",
    ],
)
