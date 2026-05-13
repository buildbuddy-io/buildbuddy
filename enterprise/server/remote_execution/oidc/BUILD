load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oidc",
    srcs = ["oidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/oidc",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/platform",
        "//server/util/status",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_google_uuid//:uuid",
    ],
)

go_test(
    name = "oidc_test",
    size = "small",
    srcs = ["oidc_test.go"],
    embed = [":oidc"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/claims",
        "//server/util/testing/flags",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_stretchr_testify//require",
    ],
)
