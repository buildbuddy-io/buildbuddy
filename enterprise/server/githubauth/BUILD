load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "githubauth",
    srcs = ["githubauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/githubauth",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/github",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/cookie",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_jwt_jwt//:jwt",
        "@com_github_google_uuid//:uuid",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
