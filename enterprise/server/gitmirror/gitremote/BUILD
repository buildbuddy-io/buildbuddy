load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gitremote",
    srcs = ["gitremote.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote",
    deps = [
        "//server/http/cachecontrol",
        "//server/http/httpclient",
        "//server/util/lru",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "gitremote_test",
    srcs = ["gitremote_test.go"],
    deps = [
        ":gitremote",
        "//server/testutil/testgit",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
