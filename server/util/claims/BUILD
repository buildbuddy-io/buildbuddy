load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "//server/util/authutil",
        "//server/util/capabilities",
        "//server/util/flag",
        "//server/util/lru",
        "//server/util/request_context",
        "//server/util/role",
        "//server/util/status",
        "//server/util/subdomain",
        "@com_github_golang_jwt_jwt//:jwt",
    ],
)

go_test(
    name = "claims_test",
    srcs = ["claims_test.go"],
    deps = [
        ":claims",
        "//server/util/authutil",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
