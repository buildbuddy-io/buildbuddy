load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "logo_dark",
    srcs = ["//static:image/logo_dark.svg"],
    outs = ["logo_dark.svg"],
    cmd_bash = "cp $(SRCS) $@",
)

go_library(
    name = "email",
    srcs = ["email.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications/email",
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
