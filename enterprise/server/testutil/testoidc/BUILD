load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testoidc",
    testonly = 1,
    srcs = ["testoidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testoidc",
    visibility = ["//visibility:public"],
)

go_test(
    name = "testoidc_test",
    srcs = ["testoidc_test.go"],
    deps = [
        ":testoidc",
        "@com_github_coreos_go_oidc_v3//oidc",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
