load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oidc",
    srcs = ["oidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oidc",
    deps = [
        "//enterprise/server/selfauth",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/cookie",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/subdomain",
        "//server/util/url",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//credentials",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//peer",
        "@org_golang_x_oauth2//:oauth2",
    ],
)

go_test(
    name = "oidc_test",
    size = "small",
    srcs = ["oidc_test.go"],
    embed = [":oidc"],
    deps = [
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/cookie",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
