load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sendgrid",
    srcs = ["sendgrid.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/sendgrid",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_sendgrid_sendgrid_go//helpers/mail",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
