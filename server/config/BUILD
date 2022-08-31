load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "config",
    srcs = [
        "config.go",
        "migration_config.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/config",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/disk",
        "//server/util/file",
        "//server/util/flagutil/yaml",
        "//server/util/log",
        "//server/util/status",
        "@com_github_pkg_errors//:errors",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
