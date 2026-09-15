load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "install",
    srcs = [
        "install.go",
        "install_darwin.go",
        "install_linux.go",
        "install_other.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/install",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:aix": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:android": [
            "//tools/bbaccess/tunnel/config",
            "//tools/bbaccess/tunnel/daemon",
        ],
        "@io_bazel_rules_go//go/platform:darwin": [
            "//tools/bbaccess/tunnel/config",
            "//tools/bbaccess/tunnel/daemon",
        ],
        "@io_bazel_rules_go//go/platform:dragonfly": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:freebsd": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:illumos": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:ios": [
            "//tools/bbaccess/tunnel/config",
            "//tools/bbaccess/tunnel/daemon",
        ],
        "@io_bazel_rules_go//go/platform:js": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//tools/bbaccess/tunnel/config",
            "//tools/bbaccess/tunnel/daemon",
        ],
        "@io_bazel_rules_go//go/platform:netbsd": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:openbsd": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:osx": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:plan9": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:qnx": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:solaris": [
            "//tools/bbaccess/tunnel/config",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//tools/bbaccess/tunnel/config",
        ],
        "//conditions:default": [],
    }),
)
