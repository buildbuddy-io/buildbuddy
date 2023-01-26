load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "webdriver_target",
    testonly = 1,
    srcs = ["webdriver_target.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/webdriver_target",
    deps = [
        "//enterprise/server/backends/userdb",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//server/real_environment",
        "//server/tables",
        "//server/testutil/app",
        "//server/testutil/testfs",
        "//server/testutil/testhealthcheck",
        "//server/testutil/testport",
        "//server/util/db",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
