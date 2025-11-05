load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocicache",
    srcs = ["ocicache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache",
    deps = [
        "//proto:ociregistry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/hash",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/partial",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/types",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)

go_test(
    name = "ocicache_test",
    timeout = "short",
    srcs = ["ocicache_test.go"],
    deps = [
        ":ocicache",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:ociregistry_go_proto",
        "//server/interfaces",
        "//server/testutil/testcache",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/random",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_containerregistry//pkg/crane",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//require",
    ],
)
