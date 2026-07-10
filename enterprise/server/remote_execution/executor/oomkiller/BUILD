load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oomkiller",
    srcs = [
        "oomkiller.go",
        "oomkiller_linux.go",
        "oomkiller_notlinux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor/oomkiller",
    deps = [
        "//enterprise/server/remote_execution/executorplatform",
        "//enterprise/server/remote_execution/oom",
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/resources",
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/status",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/cgroup",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "oomkiller_test",
    srcs = [
        "oomkiller_linux_test.go",
        "oomkiller_test.go",
    ],
    embed = [":oomkiller"],
    # TODO: enable this test on macOS once bare isolation type is supported.
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/oom",
        "//proto:remote_execution_go_proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
