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
    # Flat rather than per platform, which is what Gazelle would generate from
    # tundev_other.go's build constraint: x/sys/unix is only imported by the
    # Linux file, and an unused dependency is harmless.
    deps = [
        "@com_zx2c4_golang_wireguard//tun",
        "@org_golang_x_sys//unix",
    ],  # keep
)
