load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "empty_for_embedsrcs",
    outs = ["empty"],
    cmd_bash = "touch $@",
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = select({
        "//platforms/configs:linux_x86_64": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
            "//enterprise/vmsupport/bin:vmlinux_6.1",
        ],
        "//platforms/configs:linux_arm64": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
        ],
        "//conditions:default": [
            ":empty",  # embed fails with no embedsrcs
        ],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/vmsupport",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/fileresolver",
    ],
)
