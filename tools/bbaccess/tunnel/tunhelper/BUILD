load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "tunhelper_lib",
    srcs = [
        "tunhelper.go",
        "tunhelper_darwin.go",
        "tunhelper_linux.go",
        "tunhelper_other.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelper",
    visibility = ["//visibility:private"],
    deps = [
        "//tools/bbaccess/tunnel/tunhelperutil",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_vishvananda_netlink//:netlink",
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_binary(
    name = "tunhelper",
    embed = [":tunhelper_lib"],
    visibility = ["//visibility:public"],
)
