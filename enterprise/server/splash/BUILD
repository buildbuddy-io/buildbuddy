load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["splash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/splash",
    visibility = ["//enterprise:__subpackages__"],
    deps = ["@com_github_logrusorgru_aurora//:go_default_library"],
)
