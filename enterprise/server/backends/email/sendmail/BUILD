load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sendmail_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email/sendmail",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/email",
        "//server/config",
        "//server/util/flag",
        "//server/util/log",
    ],
)

go_binary(
    name = "sendmail",
    embed = [":sendmail_lib"],
    visibility = ["//visibility:public"],
)
