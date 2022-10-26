load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sandbox",
    srcs = ["sandbox.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/sandbox",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "sandbox_test",
    srcs = ["sandbox_test.go"],
    tags = [
        "no-sandbox",  # sandbox-exec is not compatible with Bazel's sandbox environment
    ],
    target_compatible_with = ["@platforms//os:macos"],
    deps = [
        ":sandbox",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
    ],
)
