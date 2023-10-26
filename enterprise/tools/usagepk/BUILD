load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "usagepk_lib",
    srcs = ["usagepk.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/usagepk",
    visibility = ["//visibility:private"],
    deps = [
        "//server/real_environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "usagepk",
    embed = [":usagepk_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
