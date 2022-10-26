load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "auth",
    srcs = ["auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auth",
    deps = [
        "//enterprise/server/selfauth",
        "//proto:api_key_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/nullauth",
        "//server/tables",
        "//server/util/alert",
        "//server/util/capabilities",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/random",
        "//server/util/request_context",
        "//server/util/role",
        "//server/util/status",
        "//server/util/url",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@com_github_golang_jwt_jwt//:jwt",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//credentials",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//peer",
        "@org_golang_x_oauth2//:oauth2",
    ],
)

go_test(
    name = "auth_test",
    size = "small",
    srcs = ["auth_test.go"],
    embed = [":auth"],
    deps = [
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/tables",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
