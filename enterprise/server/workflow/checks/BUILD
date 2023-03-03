load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "checks",
    srcs = ["checks.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/checks",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/webhooks/github",
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_go_github_v43//github",
    ],
)
