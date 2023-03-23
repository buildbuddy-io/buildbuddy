load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "githubapp",
    srcs = ["githubapp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/webhooks/github",
        "//proto:github_go_proto",
        "//proto:workflow_go_proto",
        "//server/backends/github",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/git",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_golang_jwt_jwt//:jwt",
        "@com_github_google_go_github_v43//github",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
