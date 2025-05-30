load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//dockerfiles/test_images:defs.bzl", "NET_TOOLS_IMAGE")

go_library(
    name = "networking",
    srcs = ["networking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/networking",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_vishvananda_netlink//:netlink",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "networking_test",
    srcs = ["networking_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://" + NET_TOOLS_IMAGE,
        "test.EstimatedComputeUnits": "4",
    },
    shard_count = 2,
    tags = [
        "docker",
        "no-sandbox",
    ],
    deps = [
        ":networking",
        "//server/testutil/testnetworking",
        "//server/testutil/testshell",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
