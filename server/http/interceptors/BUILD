load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "interceptors",
    srcs = ["interceptors.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/interceptors",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:context_go_proto",
        "//server/environment",
        "//server/http/protolet",
        "//server/metrics",
        "//server/role_filter",
        "//server/util/alert",
        "//server/util/clientip",
        "//server/util/log",
        "//server/util/request_context",
        "//server/util/subdomain",
        "//server/util/uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "interceptors_test",
    srcs = ["interceptors_test.go"],
    embed = [":interceptors"],
    deps = [
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
