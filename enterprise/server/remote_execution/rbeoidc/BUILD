load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rbeoidc",
    srcs = ["rbeoidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/rbeoidc",
    deps = [
        "//enterprise/server/oidcissuer",
        "//enterprise/server/remote_execution/awsoidc",
        "//enterprise/server/remote_execution/gcpoidc",
        "//enterprise/server/util/ci_runner_env",
        "//proto:remote_execution_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/util/flag",
        "//server/util/lru",
        "//server/util/platform",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_google_uuid//:uuid",
    ],
)

go_test(
    name = "rbeoidc_test",
    size = "small",
    srcs = ["rbeoidc_test.go"],
    embed = [":rbeoidc"],
    deps = [
        "//enterprise/server/oidcissuer",
        "//enterprise/server/remote_execution/awsoidc",
        "//enterprise/server/remote_execution/gcpoidc",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/platform",
        "//server/util/testing/flags",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
