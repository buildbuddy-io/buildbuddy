load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__,//server/static:__pkg__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/static:__pkg__",
])

go_library(
    name = "githubauth",
    srcs = ["githubauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/githubauth",
    deps = [
        "//enterprise/server/githubapp",
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
