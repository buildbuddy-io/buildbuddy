load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hit_tracker_service",
    srcs = ["hit_tracker_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_service",
    deps = [
        "//proto:hit_tracker_go_proto",
        "//server/real_environment",
    ],
)

go_test(
    name = "hit_tracker_service_test",
    size = "small",
    srcs = ["hit_tracker_service_test.go"],
    deps = [
        ":hit_tracker_service",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
