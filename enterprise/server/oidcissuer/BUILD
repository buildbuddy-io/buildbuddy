load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oidcissuer",
    srcs = ["oidcissuer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oidcissuer",
    deps = ["@com_github_golang_jwt_jwt_v4//:jwt"],
)

go_test(
    name = "oidcissuer_test",
    size = "small",
    srcs = ["oidcissuer_test.go"],
    embed = [":oidcissuer"],
    deps = [
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_stretchr_testify//require",
    ],
)
