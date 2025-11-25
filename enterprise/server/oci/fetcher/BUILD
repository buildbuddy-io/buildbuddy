load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "fetcher",
    srcs = ["fetcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:oci_fetcher_go_proto",
        "//server/http/httpclient",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "fetcher_test",
    srcs = ["fetcher_test.go"],
    deps = [
        ":fetcher",
        "//proto:oci_fetcher_go_proto",
        "//server/testutil/testregistry",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
