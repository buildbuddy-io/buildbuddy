load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "apikeyauth",
    srcs = ["apikeyauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/apikeyauth",
    deps = [
        "//enterprise/gateway/gatewayauth",
        "//server/interfaces",
    ],
)
