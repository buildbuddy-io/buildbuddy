load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "workspace",
    srcs = ["workspace_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workspace",
    deps = [
        "//proto:github_go_proto",
        "//proto:workspace_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/git",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)
