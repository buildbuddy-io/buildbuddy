load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "container_registry",
    srcs = ["container_registry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/container_registry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
        "@com_github_distribution_distribution_v3//configuration",
        "@com_github_distribution_distribution_v3//registry/handlers",
        "@com_github_distribution_distribution_v3//registry/storage/driver/filesystem",
        "@com_github_k0kubun_pp_v3//:pp",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
