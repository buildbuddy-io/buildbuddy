load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("//rules:platform_transitions.bzl", "STATIC_ON_LINUX", "embedded_static_binary")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(srcs = ["main.go"])

go_library(
    name = "goinit_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/goinit",
    deps = [
        "//enterprise/server/util/firecrackerutil",
        "//enterprise/server/util/vsock",
        "//enterprise/server/vmdns",
        "//enterprise/server/vmexec",
        "//enterprise/server/vmvfs:vmvfs_lib",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/rlimit",
        "//server/util/status",
        "@com_github_jsimonetti_rtnetlink//rtnl",
        "@com_github_miekg_dns//:dns",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "goinit",
    embed = [":goinit_lib"],
    static = STATIC_ON_LINUX,
)

# Statically linked goinit for the firecracker initrd; see embedded_static_binary.
embedded_static_binary(
    name = "goinit_static",
    actual = ":goinit",
)
