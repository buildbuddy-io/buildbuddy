load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gitmirror",
    srcs = ["gitmirror.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror",
    deps = [
        "//enterprise/server/gitmirror/gitremote",
        "//enterprise/server/gitmirror/gitstorage",
        "//server/util/flag",
        "//server/util/log",
        "@com_github_go_git_go_git_v5//plumbing/format/pktline",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_masterminds_semver_v3//:semver",
    ],
)

go_test(
    name = "gitmirror_test",
    srcs = ["gitmirror_test.go"],
    shard_count = 4,
    deps = [
        ":gitmirror",
        "//enterprise/server/gitmirror/gitremote",
        "//enterprise/server/gitmirror/gitstorage",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testhttp",
        "//server/testutil/testshell",
        "//server/util/git",
        "//server/util/testing/flags",
        "@com_github_go_git_go_git_v5//plumbing/format/pktline",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
