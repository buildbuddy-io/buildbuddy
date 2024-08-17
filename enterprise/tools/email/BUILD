load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "email_lib",
    srcs = ["email.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/email",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/sendgrid",
        "//enterprise/server/notifications/email",
        "//server/util/flag",
        "//server/util/log",
    ],
)

go_binary(
    name = "email",
    embed = [":email_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
