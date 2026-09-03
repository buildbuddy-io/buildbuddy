load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "procstats",
    srcs = [
        "procstats.go",
        "procstats_openbsd.go",
        "procstats_ps.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/lib/set",
        "@com_github_shirou_gopsutil_v3//process",
    ] + select({
        "@io_bazel_rules_go//go/platform:openbsd": [
            "//enterprise/server/util/processlist",
        ],
        "//conditions:default": [
            "@com_github_mitchellh_go_ps//:go-ps",
        ],
    }),
)

go_test(
    name = "procstats_test",
    srcs = ["procstats_test.go"],
    deps = [
        ":procstats",
        "@com_github_stretchr_testify//require",
    ],
)
