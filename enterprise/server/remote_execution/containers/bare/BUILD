load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "bare",
    srcs = ["bare.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/interfaces",
        "//server/util/status",
    ],
)

go_test(
    name = "bare_test",
    size = "small",
    srcs = ["bare_test.go"],
    deps = [
        ":bare",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
    ],
)
