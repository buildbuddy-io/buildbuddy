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
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/util/status",
        ],
        "//conditions:default": [],
    }),
)

package(default_visibility = ["//enterprise:__subpackages__"])
