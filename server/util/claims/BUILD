load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "claims",
    srcs = ["claims.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/claims",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/capabilities",
        "//server/util/flagutil",
        "//server/util/lru",
        "//server/util/request_context",
        "//server/util/role",
        "//server/util/status",
        "@com_github_golang_jwt_jwt//:jwt",
    ],
)
