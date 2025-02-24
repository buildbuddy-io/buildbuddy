load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ociregistry",
    srcs = ["ociregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
    ],
)

go_test(
    name = "ociregistry_test",
    timeout = "short",
    srcs = ["ociregistry_test.go"],
    deps = [
        ":ociregistry",
        "//server/environment",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/testutil/testregistry",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
