load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "util",
    srcs = ["ci_runner_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/util",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/cmd/ci_runner/bundle",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
