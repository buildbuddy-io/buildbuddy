load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "terminal",
    srcs = [
        "terminal.go",
        "terminal_darwin.go",
        "terminal_linux.go",
        "terminal_unix.go",
        "terminal_unsupported.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/terminal",
    deps = [
        "@com_github_mattn_go_isatty//:go-isatty",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)
