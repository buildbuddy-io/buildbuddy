load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "copy_on_write",
    srcs = ["copy_on_write.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/status",
        "@org_golang_x_exp//maps",
        "@org_golang_x_sys//unix",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "copy_on_write_test",
    srcs = ["copy_on_write_test.go"],
    deps = [
        ":copy_on_write",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "copy_on_write_benchmark_test",
    srcs = ["copy_on_write_test.go"],
    args = [
        "-test.skip=^Test",
        "-test.bench=.",
        # Each benchmark iteration is relatively expensive due to setup IO
        # costs, so run a fixed number instead of using time-based estimation.
        "-test.benchtime=16x",
    ],
    tags = ["performance"],
    deps = [
        ":copy_on_write",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "@com_github_stretchr_testify//require",
    ],
)
