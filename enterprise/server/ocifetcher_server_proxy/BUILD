load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocifetcher_server_proxy",
    srcs = ["ocifetcher_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ocifetcher_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:oci_fetcher_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "ocifetcher_server_proxy_test",
    size = "medium",
    srcs = ["ocifetcher_server_proxy_test.go"],
    embed = [":ocifetcher_server_proxy"],
    deps = [
        "//enterprise/server/oci/ocifetcher",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testregistry",
        "//proto:capability_go_proto",
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/util/claims",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
