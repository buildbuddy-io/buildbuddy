load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "splash",
    srcs = ["splash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/splash",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//server/util/log",
        "@com_github_logrusorgru_aurora//:aurora",
    ],
)
