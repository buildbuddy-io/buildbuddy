load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "blockio",
    srcs = ["blockio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@org_golang_x_exp//maps",
        "@org_golang_x_sys//unix",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "blockio_test",
    srcs = ["blockio_test.go"],
    deps = [
        ":blockio",
        "//server/testutil/testfs",
        "//server/util/disk",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "blockio_benchmark_test",
    srcs = ["blockio_test.go"],
    args = [
        "-test.skip=^Test",
        "-test.bench=.",
        # Each benchmark iteration is relatively expensive due to setup IO
        # costs, so run a fixed number instead of using time-based estimation.
        "-test.benchtime=16x",
    ],
    tags = ["performance"],
    deps = [
        ":blockio",
        "//server/testutil/testfs",
        "//server/util/disk",
        "@com_github_stretchr_testify//require",
    ],
)
