load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("//rules:platform_transitions.bzl", "linux_arm64_alias", "linux_x86_64_alias")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oci_to_ext4_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/oci_to_ext4",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/oci/ociconv",
        "//enterprise/server/util/oci",
        "//server/real_environment",
    ],
)

go_binary(
    name = "oci_to_ext4",
    embed = [":oci_to_ext4_lib"],
    pure = "on",
    static = "on",
)

linux_x86_64_alias(
    name = "oci_to_ext4_linux_amd64",
    actual = ":oci_to_ext4",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_arm64_alias(
    name = "oci_to_ext4_linux_arm64",
    actual = ":oci_to_ext4",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
