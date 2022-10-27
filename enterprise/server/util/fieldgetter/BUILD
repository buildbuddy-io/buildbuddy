load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "fieldgetter",
    srcs = ["fieldgetter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter",
)

go_test(
    name = "fieldgetter_test",
    size = "small",
    srcs = ["fieldgetter_test.go"],
    deps = [
        ":fieldgetter",
        "@com_github_stretchr_testify//assert",
    ],
)
