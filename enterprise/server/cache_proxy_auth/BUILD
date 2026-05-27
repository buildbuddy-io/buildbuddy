load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_proxy_auth",
    srcs = ["cache_proxy_auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_auth",
    visibility = ["//visibility:public"],
    deps = ["//server/util/flag"],
)
