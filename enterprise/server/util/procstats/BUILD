load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "procstats",
    srcs = ["procstats.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats",
    deps = [
        "//proto:remote_execution_go_proto",
        "@com_github_mitchellh_go_ps//:go-ps",
        "@com_github_shirou_gopsutil_v3//process",
    ],
)
