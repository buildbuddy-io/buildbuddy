load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage_service",
    srcs = ["usage_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service",
    deps = [
        "//enterprise/server/usage/config",
        "//proto:usage_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "usage_service_test",
    srcs = ["usage_service_test.go"],
    deps = [
        ":usage_service",
        "//proto:context_go_proto",
        "//proto:usage_go_proto",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "@com_github_google_go_cmp//cmp",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
