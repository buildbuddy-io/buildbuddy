load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "fetcher",
    srcs = ["fetcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher",
    deps = [
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//server/environment",
        "//server/http/httpclient",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
    ],
)

go_test(
    name = "fetcher_test",
    size = "small",
    srcs = ["fetcher_test.go"],
    deps = [
        ":fetcher",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
