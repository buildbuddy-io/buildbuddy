load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "agent_review_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/agent_review",
    visibility = ["//visibility:private"],
    deps = ["//server/util/log"],
)

go_binary(
    name = "agent_review",
    embed = [":agent_review_lib"],
    visibility = ["//visibility:public"],
)
