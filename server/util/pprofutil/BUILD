load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "pprofutil",
    srcs = ["pprofutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/pprofutil",
    deps = ["//server/util/flag"],
)

go_test(
    name = "pprofutil_test",
    srcs = ["pprofutil_test.go"],
    deps = [
        ":pprofutil",
        "//server/testutil/testpprof",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
