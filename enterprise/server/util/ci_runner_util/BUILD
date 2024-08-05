load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ci_runner_util",
    srcs = ["ci_runner_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util",
    deps = [
        "//enterprise/server/cmd/ci_runner/bundle",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
