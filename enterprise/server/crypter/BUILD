load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "crypter",
    srcs = ["crypter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter",
    deps = [
        "//proto:storage_go_proto",
        "@org_golang_x_crypto//chacha20poly1305",
    ],
)

go_test(
    name = "crypter_test",
    srcs = ["crypter_test.go"],
    deps = ["@com_github_stretchr_testify//require"],
)
