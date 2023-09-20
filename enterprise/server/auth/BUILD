load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "auth",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/githubauth",
        "//enterprise/server/oidc",
        "//enterprise/server/saml",
        "//server/environment",
        "//server/interfaces",
        "//server/nullauth",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_jwt_jwt//:jwt",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
