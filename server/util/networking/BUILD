load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "networking",
    srcs = ["networking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/networking",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@org_golang_x_sys//unix",
    ],
)
