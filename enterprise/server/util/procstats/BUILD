load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "procstats",
    srcs = [
        "procstats.go",
        "procstats_linux.go",
        "procstats_other.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/lib/set",
        "@com_github_mitchellh_go_ps//:go-ps",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "//conditions:default": [
            "@com_github_shirou_gopsutil_v3//process",
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

go_test(
    name = "procstats_linux_test",
    srcs = ["procstats_linux_test.go"],
    embed = [":procstats"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
