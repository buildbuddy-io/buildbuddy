load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "block_io",
    srcs = [
        "block_io_unix.go",
        "block_io_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:aix": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:android": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:dragonfly": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:freebsd": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:illumos": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:ios": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:netbsd": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:openbsd": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:solaris": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/util/status",
        ],
        "//conditions:default": [],
    }),
)

package(default_visibility = ["//enterprise:__subpackages__"])
