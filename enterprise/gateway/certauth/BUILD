load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "certauth",
    srcs = ["certauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/certauth",
    deps = [
        "//enterprise/gateway/gatewayauth",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/relayauth",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "certauth_test",
    srcs = ["certauth_test.go"],
    embed = [":certauth"],
    deps = [
        "//enterprise/gateway/gatewayauth",
        "//server/testutil/testauthrelay",
        "//server/util/relayauth",
        "//server/util/testing/flags",
        "//server/util/wgkeys",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)
