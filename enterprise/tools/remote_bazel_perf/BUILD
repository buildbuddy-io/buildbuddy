load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "remote_bazel_perf_lib",
    srcs = ["remote_bazel_perf.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/remote_bazel_perf",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@com_github_google_shlex//:shlex",
        "@com_github_google_uuid//:uuid",
    ],
)

go_binary(
    name = "remote_bazel_perf",
    embed = [":remote_bazel_perf_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
