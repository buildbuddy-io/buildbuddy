load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cpuset",
    srcs = ["cpuset.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/resources",
        "//server/util/priority_queue",
        "@org_golang_x_exp//slices",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cpuset_test",
    srcs = ["cpuset_test.go"],
    deps = [
        ":cpuset",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
