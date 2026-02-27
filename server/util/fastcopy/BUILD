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
            "@org_golang_x_sys//windows",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "fastcopy_test",
    srcs = [
        "fastcopy_test.go",
        "fastcopy_windows_test.go",
    ],
    args = [
        "-test.skip=^Benchmark",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "fastcopy_test",
        "test.container-image": "docker://gcr.io/flame-public/test-xfsprogs@sha256:d751cc048dfe2b343d8c78eec537933c23fe41be8e70b506434c8044db537ef5",
        "test.EstimatedFreeDiskBytes": "800MB",
    },
    pure = "on",
    static = "on",  # because test-xfsprogs image (alpine-based) doesn't have glibc.
    tags = ["docker"],
    deps = [
        ":fastcopy",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ] + select({
        "@io_bazel_rules_go//go/platform:windows": [
            "@org_golang_x_sys//windows",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "fastcopy_benchmark_test",
    srcs = ["fastcopy_windows_test.go"],
    args = [
        "-test.skip=^Test",
        "-test.bench=BenchmarkWindowsFastCopy",
        "-test.benchtime=1x",
    ],
    tags = ["performance"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":fastcopy",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sys//windows",
    ],
)
