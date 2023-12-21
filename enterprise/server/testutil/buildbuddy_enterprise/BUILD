load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "buildbuddy_enterprise",
    testonly = 1,
    srcs = ["buildbuddy_enterprise.go"],
    data = [
        "//enterprise/config:config_files",
        "//enterprise/server/cmd/server:buildbuddy",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise",
    deps = [
        "//enterprise/server/testutil/testredis",
        "//proto:context_go_proto",
        "//proto:user_go_proto",
        "//server/testutil/app",
        "//server/testutil/testport",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
    ],
)
