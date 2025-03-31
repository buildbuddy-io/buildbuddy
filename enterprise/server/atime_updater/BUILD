load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "atime_updater",
    srcs = ["atime_updater.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "atime_updater_test",
    size = "small",
    srcs = ["atime_updater_test.go"],
    embed = [":atime_updater"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
