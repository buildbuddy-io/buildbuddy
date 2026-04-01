load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "server",
    srcs = [
        "forwardingtun.go",
        "gateway.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/gateway/keys",
        "//proto:gateway_go_proto",
        "//server/environment",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_zx2c4_golang_wireguard//conn",
        "@com_zx2c4_golang_wireguard//device",
        "@com_zx2c4_golang_wireguard//tun",
        "@dev_gvisor_gvisor//pkg/buffer",
        "@dev_gvisor_gvisor//pkg/tcpip",
        "@dev_gvisor_gvisor//pkg/tcpip/header",
        "@dev_gvisor_gvisor//pkg/tcpip/link/channel",
        "@dev_gvisor_gvisor//pkg/tcpip/network/ipv4",
        "@dev_gvisor_gvisor//pkg/tcpip/network/ipv6",
        "@dev_gvisor_gvisor//pkg/tcpip/stack",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/icmp",
        "@dev_gvisor_gvisor//pkg/tcpip/adapters/gonet",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/tcp",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/udp",
        "@com_github_miekg_dns//:dns",
    ],
)

go_test(
    name = "server_test",
    srcs = ["gateway_test.go"],
    embed = [":server"],
    deps = [
        "//proto:gateway_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
