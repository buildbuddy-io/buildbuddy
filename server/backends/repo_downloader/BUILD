load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["repo_downloader.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_go_git_go_git_v5//:go_default_library",
        "@com_github_go_git_go_git_v5//config:go_default_library",
        "@com_github_go_git_go_git_v5//plumbing/transport/http:go_default_library",
        "@com_github_go_git_go_git_v5//storage/memory:go_default_library",
    ],
)
