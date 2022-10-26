load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cpio_lib",
    srcs = ["cpio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/cpio",
    deps = [
        "//server/util/log",
        "@com_github_cavaliergopher_cpio//:cpio",
    ],
)

go_binary(
    name = "cpio",
    embed = [":cpio_lib"],
)
