load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cgroup",
    srcs = ["cgroup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/log",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cgroup_test",
    srcs = ["cgroup_test.go"],
    embed = [":cgroup"],
    deps = [
        "//proto:remote_execution_go_proto",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
