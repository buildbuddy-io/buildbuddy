load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//dockerfiles/test_images:defs.bzl", "NET_TOOLS_IMAGE")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usernet",
    srcs = ["usernet.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/usernet",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/networking",
        "//server/util/status",
        "@com_github_vishvananda_netlink//:netlink",
        "@dev_gvisor_gvisor//pkg/buffer",
        "@dev_gvisor_gvisor//pkg/tcpip",
        "@dev_gvisor_gvisor//pkg/tcpip/adapters/gonet",
        "@dev_gvisor_gvisor//pkg/tcpip/checksum",
        "@dev_gvisor_gvisor//pkg/tcpip/header",
        "@dev_gvisor_gvisor//pkg/tcpip/link/fdbased",
        "@dev_gvisor_gvisor//pkg/tcpip/network/arp",
        "@dev_gvisor_gvisor//pkg/tcpip/network/ipv4",
        "@dev_gvisor_gvisor//pkg/tcpip/stack",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/icmp",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/tcp",
        "@dev_gvisor_gvisor//pkg/tcpip/transport/udp",
        "@dev_gvisor_gvisor//pkg/waiter",
        "@org_golang_x_net//icmp",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "usernet_test",
    srcs = ["usernet_test.go"],
    embed = [":usernet"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://" + NET_TOOLS_IMAGE,
        "test.EstimatedComputeUnits": "4",
        "test.no-sandbox": "",
    },
    tags = ["docker"],
    deps = [
        "//server/testutil/testnetworking",
        "//server/util/networking",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@dev_gvisor_gvisor//pkg/tcpip",
        "@dev_gvisor_gvisor//pkg/tcpip/header",
        "@org_golang_x_net//icmp",
    ],
)
