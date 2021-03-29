load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "repo_downloader",
    srcs = ["repo_downloader.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/git",
        "@com_github_go_git_go_git_v5//:go-git",
        "@com_github_go_git_go_git_v5//config",
        "@com_github_go_git_go_git_v5//storage/memory",
    ],
)
