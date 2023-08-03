load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(default_visibility = ["//enterprise:__subpackages__"])

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/vmsupport/bin:initrd.cpio",
            "//enterprise/vmsupport/bin:vmlinux",
        ],
        "//conditions:default": [],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/vmsupport",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/fileresolver",
    ],
)
