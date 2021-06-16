load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testgit",
    testonly = 1,
    srcs = ["testgit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_go_git_go_git_v5//:go-git",
        "@com_github_go_git_go_git_v5//plumbing",
        "@com_github_go_git_go_git_v5//plumbing/object",
    ],
)
