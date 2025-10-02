load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "remote_crypter",
    srcs = ["remote_crypter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter",
    deps = [
        "//enterprise/server/crypter_key_cache",
        "//proto:encryption_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
    ],
)

go_test(
    name = "remote_crypter_test",
    srcs = ["remote_crypter_test.go"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)
