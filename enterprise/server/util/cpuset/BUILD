load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cpuset",
    srcs = [
        "cpuset.go",
        "numcpu_darwin.go",
        "numcpu_linux.go",
        "numcpu_windows.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/priority_queue",
        "@org_golang_x_exp//constraints",
        "@org_golang_x_exp//slices",
    ] + select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "@com_github_elastic_gosigar//:gosigar",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "@com_github_prometheus_procfs//:procfs",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "@com_github_elastic_gosigar//:gosigar",
        ],
        "//conditions:default": [],
    }),
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cpuset_test",
    srcs = ["cpuset_test.go"],
    deps = [
        ":cpuset",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
