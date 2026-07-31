load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "testhealth",
    srcs = ["testhealth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testhealth",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/flaghistory",
        "//cli/log",
        "//cli/login",
        "//cli/workspace",
        "//proto:test_health_go_proto",
        "//server/test_health/identity",
        "//server/test_health/junit",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/status",
        "//server/util/uuid",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "testhealth_test",
    srcs = ["testhealth_test.go"],
    deps = [
        ":testhealth",
        "//proto:test_health_go_proto",
        "//server/test_health/junit",
        "//server/test_health/normalize",
        "@com_github_stretchr_testify//require",
    ],
)
