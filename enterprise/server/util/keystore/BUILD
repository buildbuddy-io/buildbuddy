load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "keystore",
    srcs = ["keystore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore",
    deps = [
        "//server/environment",
        "//server/util/status",
        "@org_golang_x_crypto//nacl/box",
    ],
)

go_test(
    name = "keystore_test",
    srcs = ["keystore_test.go"],
    deps = [
        ":keystore",
        "//enterprise/server/backends/kms",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
