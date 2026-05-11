load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "auth",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/githubauth",
        "//enterprise/server/oidc",
        "//enterprise/server/saml",
        "//server/backends/github",
        "//server/interfaces",
        "//server/nullauth",
        "//server/real_environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_jwt_jwt_v4//:jwt",
    ],
)

go_test(
    name = "auth_test",
    size = "small",
    srcs = ["auth_test.go"],
    embed = [":auth"],
    deps = [
        "//enterprise/server/oidc",
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/nullauth",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
