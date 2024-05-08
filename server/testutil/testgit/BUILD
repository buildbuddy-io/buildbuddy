load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "testgit",
    testonly = 1,
    srcs = ["testgit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testgit",
    deps = [
        "//proto:github_go_proto",
        "//proto:repo_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "//server/util/status",
        "@com_github_google_go_github_v59//github",
    ],
)
