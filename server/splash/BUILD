load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "splash",
    srcs = ["splash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/splash",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@com_github_logrusorgru_aurora//:aurora",
    ],
)
