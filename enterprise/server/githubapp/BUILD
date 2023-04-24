load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "githubapp",
    srcs = ["githubapp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/webhooks/github",
        "//enterprise/server/webhooks/webhook_data",
        "//proto:github_go_proto",
        "//proto:workflow_go_proto",
        "//server/backends/github",
        "//server/environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/git",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/retry",
        "//server/util/role",
        "//server/util/status",
        "@com_github_golang_jwt_jwt//:jwt",
        "@com_github_google_go_github_v43//github",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
