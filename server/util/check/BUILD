load("@io_bazel_rules_go//go:def.bzl", "go_library")

config_setting(
    name = "opt",
    values = {"compilation_mode": "opt"},
)

go_library(
    name = "check",
    srcs = select({
        ":opt": ["check_opt.go"],
        "//conditions:default": ["check_debug.go"],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/check",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)
