load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "disk",
    srcs = [
        "disk.go",
        "disk_linux.go",
        "disk_unix.go",
        "disk_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/disk",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/metrics",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/tracing",
        "@org_golang_x_sync//semaphore",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "@org_golang_x_sys//windows",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "disk_test",
    srcs = [
        "disk_linux_test.go",
        "disk_test.go",
    ],
    deps = [
        ":disk",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/testutil/testmount",
        ],
        "//conditions:default": [],
    }),
)
