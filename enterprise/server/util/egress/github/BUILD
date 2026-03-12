load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["github.csv"])

go_binary(
    name = "github",
    embed = [":github_lib"],
    visibility = ["//enterprise/server/util/egress:__pkg__"],
)

go_library(
    name = "github_lib",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/github",
)
