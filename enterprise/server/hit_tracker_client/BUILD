load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hit_tracker_client",
    srcs = ["hit_tracker_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_client",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
    ],
)

go_test(
    name = "hit_tracker_client_test",
    srcs = ["hit_tracker_client_test.go"],
    deps = [
        ":hit_tracker_client",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
