load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ociregistry",
    srcs = ["ociregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/ocicache",
        "//proto:ociregistry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/http/httpclient",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "ociregistry_test",
    timeout = "short",
    srcs = ["ociregistry_test.go"],
    deps = [
        ":ociregistry",
        "//enterprise/server/clientidentity",
        "//server/interfaces",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/testutil/testregistry",
        "//server/util/random",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
