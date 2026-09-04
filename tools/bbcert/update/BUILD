load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "update",
    srcs = [
        "reexec_other.go",
        "reexec_unix.go",
        "run.go",
        "update.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbcert/update",
    visibility = ["//tools/bbcert:__subpackages__"],
)

go_test(
    name = "update_test",
    srcs = ["update_test.go"],
    embed = [":update"],
    deps = ["@com_github_stretchr_testify//require"],
)
