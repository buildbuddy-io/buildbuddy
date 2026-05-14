load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vertexai",
    srcs = ["vertexai.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/vertexai",
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_x_oauth2//google",
    ],
)
