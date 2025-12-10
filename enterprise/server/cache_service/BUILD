load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_service",
    srcs = ["cache_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_service",
    deps = [
        "//proto:cache_go_proto",
        "//proto:cache_service_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/prefix",
    ],
)
