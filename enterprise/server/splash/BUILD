load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "splash",
    srcs = ["splash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/splash",
    deps = [
        "//server/util/log",
        "@com_github_logrusorgru_aurora//:aurora",
    ],
)
