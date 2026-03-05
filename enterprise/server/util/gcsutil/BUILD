load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcsutil",
    srcs = ["gcsutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/gcsutil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:storage_go_proto",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
