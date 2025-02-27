load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "networking",
    srcs = ["networking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/networking",
    visibility = ["//visibility:public"],
    deps = [
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
        "test.container-image": "docker://gcr.io/flame-public/net-tools@sha256:e904e2149194b94f4504fdfb3c7b2b71afa130708feb482fe9a9d557453fa8fd",
        "test.EstimatedComputeUnits": "2",
    },
    tags = [
        "docker",
        "no-sandbox",
    ],
    deps = [
        ":networking",
        "//server/testutil/testnetworking",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
