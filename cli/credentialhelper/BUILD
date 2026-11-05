load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "credentialhelper",
    srcs = ["credentialhelper.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/credentialhelper",
    deps = ["@org_golang_google_grpc//:grpc"],
)

go_test(
    name = "credentialhelper_test",
    srcs = ["credentialhelper_test.go"],
    embed = [":credentialhelper"],
    deps = ["@com_github_stretchr_testify//require"],
)
