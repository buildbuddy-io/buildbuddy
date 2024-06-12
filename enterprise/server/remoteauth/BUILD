load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "//server/util/hash",
        "//server/util/lru",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
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
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@org_golang_google_grpc//metadata",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
