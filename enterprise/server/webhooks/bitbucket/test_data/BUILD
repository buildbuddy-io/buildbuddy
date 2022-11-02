load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise/server/webhooks/bitbucket:__subpackages__"])

go_library(
    name = "test_data",
    srcs = ["test_data.go"],
    embedsrcs = [
        "pull_request_event.json",
        "push_event.json",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket/test_data",
)
