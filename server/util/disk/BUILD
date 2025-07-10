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
    exec_properties = select({
        # Run in firecracker to work around "maximum fs stacking depth exceeded"
        # errors when creating nested overlayfs mounts.
        "@io_bazel_rules_go//go/platform:linux": {
            "test.workload-isolation-type": "firecracker",
        },
        "//conditions:default": {},
    }),
    deps = [
        ":disk",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/testutil/testmount",
        ],
        "//conditions:default": [],
    }),
)
