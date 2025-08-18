load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "remote_crypter",
    srcs = ["remote_crypter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter",
    deps = [
        "//enterprise/server/crypter",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "//proto:encryption_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:storage_go_proto",
        "//third_party/singleflight",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "remote_crypter_test",
    srcs = ["remote_crypter_test.go"],
    deps = [
        ":remote_crypter",
        "@com_github_stretchr_testify//require",
    ],
)
