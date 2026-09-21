load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
    # Flat rather than the per-platform select Gazelle generates from the
    # build constraints: both packages build everywhere, and the stub for
    # other platforms simply uses fewer of them.
    deps = [
        "//tools/bbaccess/tunnel/daemon",
        "//tools/bbaccess/tunnel/tunnelconfig",
    ],  # keep
)
