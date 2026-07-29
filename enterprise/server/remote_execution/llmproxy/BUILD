load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "llmproxy",
    srcs = [
        "proxy.go",
        "service.go",
        "sessions.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/llmproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/redact",
        "@com_github_klauspost_compress//zstd",
    ],
)
