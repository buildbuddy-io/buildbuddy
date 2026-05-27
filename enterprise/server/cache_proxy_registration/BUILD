load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_proxy_registration",
    srcs = ["cache_proxy_registration.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_registration",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/cache_proxy_auth",
        "//proto:cache_proxy_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/version",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "cache_proxy_registration_test",
    srcs = ["cache_proxy_registration_test.go"],
    embed = [":cache_proxy_registration"],
    deps = [
        "//enterprise/server/cache_proxy_registry_server",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testredis",
        "//proto:cache_proxy_go_proto",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/claims",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
