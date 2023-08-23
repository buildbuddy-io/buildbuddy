load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcplink",
    srcs = ["gcplink.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/gcplink",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/keystore",
        "//proto:context_go_proto",
        "//proto:secrets_go_proto",
        "//server/environment",
        "//server/util/cookie",
        "//server/util/request_context",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
