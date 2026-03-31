load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ci_runner_util",
    srcs = ["ci_runner_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util",
    deps = [
        "//enterprise/server/cmd/ci_runner/bundle",
        "//enterprise/server/githubapp",
        "//enterprise/server/util/ci_runner_env",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/bb",
        "//server/util/platform",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "ci_runner_util_test",
    size = "small",
    srcs = ["ci_runner_util_test.go"],
    embed = [":ci_runner_util"],
    deps = [
        "//enterprise/server/githubapp",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/util/ci_runner_env",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testgit",
        "//server/util/platform",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
