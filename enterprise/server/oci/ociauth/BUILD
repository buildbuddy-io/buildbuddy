load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ociauth",
    srcs = ["ociauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociauth",
    deps = [
        "//proto:registry_go_proto",
        "//server/util/hash",
        "@com_github_google_go_containerregistry//pkg/name",
    ],
)

go_test(
    name = "ociauth_test",
    srcs = ["ociauth_test.go"],
    embed = [":ociauth"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)
