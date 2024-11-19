load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cgroup",
    srcs = ["cgroup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/block_io",
            "//proto:remote_execution_go_proto",
            "//proto:scheduler_go_proto",
            "//server/util/log",
            "//server/util/status",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "cgroup_test",
    srcs = ["cgroup_test.go"],
    embed = [":cgroup"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/block_io",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/util/proto",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
