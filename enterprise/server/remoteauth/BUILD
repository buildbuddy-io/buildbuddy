load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "remoteauth",
    srcs = ["remoteauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:auth_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "//server/util/subdomain",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "remoteauth_test",
    size = "small",
    srcs = ["remoteauth_test.go"],
    embed = [":remoteauth"],
    deps = [
        "//proto:auth_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/status",
        "//server/util/subdomain",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
