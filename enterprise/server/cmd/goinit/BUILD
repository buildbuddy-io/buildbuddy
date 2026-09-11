load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

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
    # "pure" causes the init file to require an ELF interpreter which the kernel
    # does not have access to if we are bulding statically.
    pure = select({
        "//:static": "auto",
        "//conditions:default": "on",
    }),
    # static = select({
    #    "//:static": "on",
    #    "//conditions:default": "auto",
    # }),
)

sh_test(
    name = "goinit_linux_amd64_size_test",
    srcs = ["goinit_size_test.sh"],
    args = [
        "$(rootpath :goinit)",
        # The current Linux amd64 binary is 37,609,632 bytes. Keep a small
        # amount of headroom for toolchain movement while catching accidental
        # dependency or embedded-data growth.
        "40000000",
    ],
    data = [":goinit"],
    target_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
)
