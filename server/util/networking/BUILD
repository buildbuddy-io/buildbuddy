load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "networking",
    srcs = ["networking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/networking",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "//server/util/background",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/uuid",
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
        "test.container-image": "docker://gcr.io/flame-public/net-tools@sha256:ac701954d2c522d0d2b5296323127cacaaf77627e69db848a8d6ecb53149d344",
        "test.EstimatedComputeUnits": "2",
    },
    tags = [
        "docker",
        "no-sandbox",
    ],
    deps = [
        ":networking",
        "//server/testutil/testnetworking",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
