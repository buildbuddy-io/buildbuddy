load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "review",
    srcs = ["review.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent/review",
    deps = [
        "//cli/agent/agentflags",
        "//cli/log",
        "//cli/terminal",
        "//cli/util/agent",
        "//cli/util/agent/agentutil",
        "@com_github_google_go_github_v59//github",
    ],
)
