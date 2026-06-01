load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:default_visibility //enterprise:__subpackages__
package(default_visibility = [
    "//enterprise:__subpackages__",
])

go_library(
    name = "cache_proxy_registry_server",
    srcs = ["cache_proxy_registry_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_registry_server",
    deps = [
        "//proto:cache_proxy_go_proto",
        "//proto:capability_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "cache_proxy_registry_server_test",
    srcs = ["cache_proxy_registry_server_test.go"],
    embed = [":cache_proxy_registry_server"],
    deps = [
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
        "//server/util/proto",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
