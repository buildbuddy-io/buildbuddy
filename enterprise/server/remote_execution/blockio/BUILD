load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "blockio",
    srcs = ["blockio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_boljen_go_bitmap//:go-bitmap",
        "@org_golang_x_sys//unix",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "blockio_test",
    srcs = ["blockio_test.go"],
    deps = [
        ":blockio",
        "//server/testutil/testfs",
        "@com_github_boljen_go_bitmap//:go-bitmap",
        "@com_github_stretchr_testify//require",
    ],
)
