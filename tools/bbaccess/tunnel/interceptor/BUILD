load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "interceptor",
    srcs = ["interceptor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/interceptor",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//tools/bbaccess/tunnel/config",
        "//tools/bbaccess/tunnel/fakeip",
        "@com_zx2c4_golang_wireguard//tun",
        "@dev_gvisor_gvisor//pkg/buffer",
        "@dev_gvisor_gvisor//pkg/tcpip",
        "@dev_gvisor_gvisor//pkg/tcpip/adapters/gonet",
        "@dev_gvisor_gvisor//pkg/tcpip/header",
        "@dev_gvisor_gvisor//pkg/tcpip/link/channel",
        "@dev_gvisor_gvisor//pkg/tcpip/network/ipv4",
        "@dev_gvisor_gvisor//pkg/tcpip/network/ipv6",
        "@dev_gvisor_gvisor//pkg/tcpip/stack",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/tcp",
        "@dev_gvisor_gvisor//pkg/waiter",
    ],
)

go_test(
    name = "interceptor_test",
    srcs = ["interceptor_test.go"],
    embed = [":interceptor"],
    deps = [
        "//tools/bbaccess/tunnel/config",
        "//tools/bbaccess/tunnel/fakeip",
        "@com_github_stretchr_testify//require",
        "@com_zx2c4_golang_wireguard//tun",
        "@dev_gvisor_gvisor//pkg/tcpip",
        "@dev_gvisor_gvisor//pkg/tcpip/header",
    ],
)
