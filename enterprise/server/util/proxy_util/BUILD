load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "proxy_util",
    srcs = ["proxy_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/http/interceptors",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/authutil",
        "//server/util/bazel_request",
        "//server/util/cdc",
        "//server/util/clientip",
        "//server/util/findmissing",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/usageutil",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "proxy_util_test",
    srcs = ["proxy_util_test.go"],
    deps = [
        ":proxy_util",
        "//enterprise/server/clientidentity",
        "//server/environment",
        "//server/http/interceptors",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/clientip",
        "//server/util/healthcheck",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
