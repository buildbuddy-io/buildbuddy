load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "commandutil",
    srcs = ["commandutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mitchellh_go_ps//:go-ps",
    ],
)

go_test(
    name = "commandutil_test",
    size = "small",
    srcs = ["commandutil_test.go"],
    data = ["//enterprise/server/remote_execution/commandutil/test_binary"],
    deps = [
        ":commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
