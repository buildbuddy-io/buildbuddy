load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "tundev",
    srcs = [
        "tundev_darwin.go",
        "tundev_linux.go",
        "tundev_other.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tundev",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:aix": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:android": [
            "@com_zx2c4_golang_wireguard//tun",
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:darwin": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:dragonfly": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:freebsd": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:illumos": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:ios": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:js": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_zx2c4_golang_wireguard//tun",
            "@org_golang_x_sys//unix",
        ],
        "@io_bazel_rules_go//go/platform:netbsd": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:openbsd": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:osx": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:plan9": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:qnx": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:solaris": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "@com_zx2c4_golang_wireguard//tun",
        ],
        "//conditions:default": [],
    }),
)
