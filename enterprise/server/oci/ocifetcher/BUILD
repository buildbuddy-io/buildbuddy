load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocifetcher",
    srcs = ["ocifetcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher",
    deps = [
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//server/environment",
        "//server/http/httpclient",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "ocifetcher_test",
    srcs = ["ocifetcher_test.go"],
    deps = [
        ":ocifetcher",
        "//enterprise/server/testutil/testregistry",
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//server/environment",
        "//server/testutil/testenv",
        "//server/testutil/testhttp",
        "//server/util/status",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
