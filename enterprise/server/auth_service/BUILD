load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "auth_service",
    srcs = ["auth_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth_service",
    deps = [
        "//proto:auth_go_proto",
        "//server/real_environment",
    ],
)

go_test(
    name = "auth_service_test",
    size = "small",
    srcs = ["auth_service_test.go"],
    embed = [":auth_service"],
    deps = [
        "//proto:auth_go_proto",
        "@com_github_stretchr_testify//assert",
    ],
)
