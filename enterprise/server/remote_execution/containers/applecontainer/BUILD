load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "applecontainer",
    srcs = ["applecontainer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/applecontainer",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
    ],
)

go_test(
    name = "applecontainer_test",
    srcs = ["applecontainer_test.go"],
    tags = ["applecontainer"],
    deps = [
        ":applecontainer",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
