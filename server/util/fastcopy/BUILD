load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "fastcopy",
    srcs = [
        "fastcopy.go",
        "fastcopy_darwin.go",
        "fastcopy_linux.go",
        "fastcopy_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fastcopy",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/util/status",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "fastcopy_test",
    srcs = ["fastcopy_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "fastcopy_test",
    },
    tags = ["docker"],
    deps = [
        ":fastcopy",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
