load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "xcode",
    srcs = [
        "xcode.go",
        "xcode_darwin.go",
    ],
    cgo = True,
    clinkopts = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "-framework CoreServices",
        ],
        "//conditions:default": [],
    }),
    importpath = "github.com/buildbuddy-io/buildbuddy/server/xcode",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//server/util/log",
            "//server/util/status",
            "@com_github_groob_plist//:plist",
        ],
        "//conditions:default": [],
    }),
)
