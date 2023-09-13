load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "uffd",
    srcs = ["uffd.go"],
    cgo = True,
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/blockio",
            "//server/util/log",
            "//server/util/status",
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

package(default_visibility = ["//enterprise:__subpackages__"])
