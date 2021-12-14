load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fastcopy",
    srcs = [
        "fastcopy.go",
        "fastcopy_darwin.go",
    ],
    cgo = True,
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fastcopy",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:aix": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:android": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:dragonfly": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:freebsd": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:illumos": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:js": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:netbsd": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:openbsd": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:plan9": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:solaris": [
            "//server/util/tracing/os",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/util/tracing/os",
        ],
        "//conditions:default": [],
    }),
)
