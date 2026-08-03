load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gitstorage",
    srcs = ["gitstorage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitstorage",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/gitmirror/gitremote",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "gitstorage_test",
    srcs = ["gitstorage_test.go"],
    deps = [
        ":gitstorage",
        "//enterprise/server/gitmirror/gitremote",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
