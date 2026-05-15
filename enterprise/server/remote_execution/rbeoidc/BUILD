load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rbeoidc",
    srcs = ["rbeoidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/rbeoidc",
    deps = [
        "//enterprise/server/util/ci_runner_env",
        "//proto:remote_execution_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/platform",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/ecr",
        "@com_github_aws_aws_sdk_go//service/sts",
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
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/claims",
        "//server/util/platform",
        "//server/util/testing/flags",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
