load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "keys",
    srcs = ["keys.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys",
    deps = ["//enterprise/server/filestore"],
)

go_test(
    name = "keys_test",
    srcs = ["keys_test.go"],
    deps = [
        ":keys",
        "@com_github_stretchr_testify//assert",
    ],
)
