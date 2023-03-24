load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "githubapp",
    srcs = ["githubapp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/github",
        "//server/environment",
        "//server/util/status",
        "@com_github_google_go_github_v43//github",
    ],
)
