load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "credentials",
    srcs = ["credentials.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/container/credentials",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:registry_go_proto",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_distribution//reference",
    ],
)

go_test(
    name = "credentials_test",
    size = "small",
    srcs = ["credentials_test.go"],
    deps = [
        ":credentials",
        "//enterprise/server/remote_execution/platform",
        "//proto:registry_go_proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
