load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "local_crypter",
    srcs = ["local_crypter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/local_crypter",
    deps = [
        "//enterprise/server/crypter",
        "//proto:encryption_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/status",
        "@org_golang_x_crypto//hkdf",
    ],
)

go_test(
    name = "local_crypter_test",
    srcs = ["local_crypter_test.go"],
    deps = [
        ":local_crypter",
        "//enterprise/server/remote_crypter",
        "//proto:remote_execution_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testdata",
        "//server/testutil/testenv",
        "//server/util/ioutil",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
