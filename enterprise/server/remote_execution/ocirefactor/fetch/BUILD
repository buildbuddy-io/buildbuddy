load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "fetch",
    srcs = ["fetch.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/ocirefactor/fetch",
    deps = [
        "//proto:ociregistry_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "fetch_test",
    timeout = "short",
    srcs = ["fetch_test.go"],
    deps = [
        ":fetch",
        "//proto:ociregistry_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testcache",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/random",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//require",
    ],
)
